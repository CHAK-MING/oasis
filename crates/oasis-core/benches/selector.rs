use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use pest::Parser;
use roaring::RoaringBitmap;
use std::collections::HashMap;

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
WHITESPACE = _{ " " | "\t" | "\r" | "\n" }

selector = { SOI ~ logical_or ~ EOI }
logical_or = { logical_and ~ ("or" ~ logical_and)* }
logical_and = { logical_not ~ ("and" ~ logical_not)* }
logical_not = { ("not" ~ primary) | primary }
primary = {
    ("(" ~ logical_or ~ ")") |
    sys_eq |
    label_eq |
    in_groups |
    all_true |
    id_list
}

all_true = { ("all" | "true") }

id_list = { id ~ (comma ~ id)* }
comma = _{ "," }

id_char = _{ ASCII_ALPHANUMERIC | "-" | "_" | "." }
id = @{ id_char+ }

sys_eq = { "system" ~ "[" ~ string ~ "]" ~ "==" ~ string }
label_eq = { "labels" ~ "[" ~ string ~ "]" ~ "==" ~ string }
in_groups = { string ~ "in" ~ "groups" }

string = @{ dquote ~ (!dquote ~ ANY)* ~ dquote | squote ~ (!squote ~ ANY)* ~ squote }
dquote = _{ "\"" }
squote = _{ "'" }
"#]
struct SelectorParser;

#[derive(Debug, Clone)]
enum SelectorAst {
    And(Box<SelectorAst>, Box<SelectorAst>),
    Or(Box<SelectorAst>, Box<SelectorAst>),
    Not(Box<SelectorAst>),
    AllTrue,
    IdList(Vec<String>),
    SystemEq(String, String),
    LabelEq(String, String),
    InGroups(String),
}

#[derive(Clone)]
struct Fixture {
    all: RoaringBitmap,
    id32_by_id: HashMap<String, u32>,
    index_labels: HashMap<(String, String), RoaringBitmap>,
    index_system: HashMap<(String, String), RoaringBitmap>,
    index_groups: HashMap<String, RoaringBitmap>,
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 {
        let b = s.as_bytes();
        let first = b[0] as char;
        let last = b[b.len() - 1] as char;
        if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

fn parse_to_ast(expression: &str) -> SelectorAst {
    let mut pairs = SelectorParser::parse(Rule::selector, expression).unwrap();
    let expr_pair = pairs.next().unwrap();
    let mut inner = expr_pair.into_inner();
    let logical_or_pair = inner.next().unwrap();
    parse_logical_or(logical_or_pair)
}

fn parse_logical_or(pair: pest::iterators::Pair<Rule>) -> SelectorAst {
    let mut inner = pair.into_inner();
    let first = parse_logical_and(inner.next().unwrap());
    let mut result = first;
    for and_pair in inner {
        let right = parse_logical_and(and_pair);
        result = SelectorAst::Or(Box::new(result), Box::new(right));
    }
    result
}

fn parse_logical_and(pair: pest::iterators::Pair<Rule>) -> SelectorAst {
    let mut inner = pair.into_inner();
    let first = parse_logical_not(inner.next().unwrap());
    let mut result = first;
    for not_pair in inner {
        let right = parse_logical_not(not_pair);
        result = SelectorAst::And(Box::new(result), Box::new(right));
    }
    result
}

fn parse_logical_not(pair: pest::iterators::Pair<Rule>) -> SelectorAst {
    let pair_str = pair.as_str().trim();
    if pair_str.starts_with("not ") {
        let mut inner = pair.into_inner();
        let primary_pair = inner.next().unwrap();
        let inner_ast = parse_primary(primary_pair);
        SelectorAst::Not(Box::new(inner_ast))
    } else {
        let mut inner = pair.into_inner();
        let primary_pair = inner.next().unwrap();
        parse_primary(primary_pair)
    }
}

fn parse_primary(pair: pest::iterators::Pair<Rule>) -> SelectorAst {
    let mut inner = pair.into_inner();
    let first_pair = inner.next().unwrap();
    match first_pair.as_rule() {
        Rule::logical_or => parse_logical_or(first_pair),
        Rule::all_true => SelectorAst::AllTrue,
        Rule::id_list => {
            let mut ids = Vec::new();
            for p in first_pair.into_inner() {
                if p.as_rule() == Rule::id {
                    ids.push(p.as_str().to_string());
                }
            }
            SelectorAst::IdList(ids)
        }
        Rule::sys_eq => {
            let mut it = first_pair.into_inner();
            let sk = unquote(it.next().unwrap().as_str());
            let sv = unquote(it.next().unwrap().as_str());
            SelectorAst::SystemEq(sk, sv)
        }
        Rule::label_eq => {
            let mut it = first_pair.into_inner();
            let lk = unquote(it.next().unwrap().as_str());
            let lv = unquote(it.next().unwrap().as_str());
            SelectorAst::LabelEq(lk, lv)
        }
        Rule::in_groups => {
            let mut it = first_pair.into_inner();
            let group_name = unquote(it.next().unwrap().as_str());
            SelectorAst::InGroups(group_name)
        }
        _ => unreachable!(),
    }
}

fn evaluate_ast(f: &Fixture, ast: &SelectorAst) -> RoaringBitmap {
    match ast {
        SelectorAst::And(left, right) => evaluate_ast(f, left) & evaluate_ast(f, right),
        SelectorAst::Or(left, right) => evaluate_ast(f, left) | evaluate_ast(f, right),
        SelectorAst::Not(inner) => f.all.clone() - evaluate_ast(f, inner),
        SelectorAst::AllTrue => f.all.clone(),
        SelectorAst::IdList(ids) => {
            let mut bm = RoaringBitmap::new();
            for id in ids {
                if let Some(id32) = f.id32_by_id.get(id) {
                    bm.insert(*id32);
                }
            }
            bm
        }
        SelectorAst::SystemEq(key, value) => f
            .index_system
            .get(&(key.clone(), value.clone()))
            .cloned()
            .unwrap_or_default(),
        SelectorAst::LabelEq(key, value) => f
            .index_labels
            .get(&(key.clone(), value.clone()))
            .cloned()
            .unwrap_or_default(),
        SelectorAst::InGroups(group) => f.index_groups.get(group).cloned().unwrap_or_default(),
    }
}

fn build_fixture(n: u32) -> Fixture {
    let mut all = RoaringBitmap::new();
    let mut id32_by_id = HashMap::with_capacity(n as usize);
    let mut index_labels: HashMap<(String, String), RoaringBitmap> = HashMap::new();
    let mut index_system: HashMap<(String, String), RoaringBitmap> = HashMap::new();
    let mut index_groups: HashMap<String, RoaringBitmap> = HashMap::new();

    for i in 0..n {
        all.insert(i);
        let agent_id = format!("agent-{:04}", i);
        id32_by_id.insert(agent_id, i);

        let role = if i % 2 == 0 {
            "web"
        } else if i % 4 == 1 {
            "db"
        } else {
            "cache"
        };
        index_labels
            .entry(("role".to_string(), role.to_string()))
            .or_default()
            .insert(i);

        let os = if i % 5 == 0 { "windows" } else { "linux" };
        index_system
            .entry(("os".to_string(), os.to_string()))
            .or_default()
            .insert(i);

        let group = if i % 10 == 0 {
            "group-a"
        } else if i % 10 == 1 {
            "group-b"
        } else {
            "group-c"
        };
        index_groups.entry(group.to_string()).or_default().insert(i);
    }

    Fixture {
        all,
        id32_by_id,
        index_labels,
        index_system,
        index_groups,
    }
}

fn bench_selector_parse(c: &mut Criterion) {
    let exprs = [
        "all",
        "labels[\"role\"] == \"web\"",
        "system[\"os\"] == \"linux\"",
        "\"group-a\" in groups",
        "labels[\"role\"] == \"web\" and system[\"os\"] == \"linux\"",
        "not (labels[\"role\"] == \"db\") and (\"group-a\" in groups or \"group-b\" in groups)",
        "agent-0001,agent-0002,agent-0003,agent-0999",
    ];

    for expr in exprs {
        c.bench_function(&format!("selector/parse/{}", expr), |b| {
            b.iter(|| black_box(parse_to_ast(black_box(expr))))
        });
    }
}

fn bench_selector_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("selector/eval");

    let cases = [
        ("all", "all"),
        ("label_web", "labels[\"role\"] == \"web\""),
        ("sys_linux", "system[\"os\"] == \"linux\""),
        ("group_a", "\"group-a\" in groups"),
        (
            "and",
            "labels[\"role\"] == \"web\" and system[\"os\"] == \"linux\"",
        ),
        (
            "complex",
            "not (labels[\"role\"] == \"db\") and (\"group-a\" in groups or \"group-b\" in groups)",
        ),
    ];

    for n in [1000u32, 5000u32, 10000u32] {
        let fixture = build_fixture(n);
        for (name, expr) in cases {
            let ast = parse_to_ast(expr);
            group.bench_with_input(BenchmarkId::new(name, n), &ast, |b, ast| {
                b.iter(|| black_box(evaluate_ast(black_box(&fixture), black_box(ast))))
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_selector_parse, bench_selector_eval);
criterion_main!(benches);
