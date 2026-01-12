use base64::Engine;

pub const OUTPUT_PREVIEW_MAX_LINES: usize = 3;
pub const OUTPUT_PREVIEW_MAX_CHARS_PER_LINE: usize = 40;

fn decode_b64_output(s: &str) -> Option<(String, bool)> {
    let rest = s.strip_prefix("base64:")?;

    const MAX_DECODE_BYTES: usize = 16 * 1024;
    let max_full_blocks = MAX_DECODE_BYTES / 3;
    let mut max_chars = max_full_blocks.saturating_mul(4);
    if max_chars > rest.len() {
        max_chars = rest.len();
    }
    max_chars -= max_chars % 4;
    let chunk = &rest[..max_chars];

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(chunk)
        .ok()?;
    let decoded_str = String::from_utf8_lossy(&decoded).to_string();
    let incomplete = chunk.len() < rest.len();
    Some((decoded_str, incomplete))
}

pub fn truncate_output_preview(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    let (text, decoded_incomplete) = match decode_b64_output(s) {
        Some((decoded, incomplete)) => (decoded, incomplete),
        None => (s.to_string(), false),
    };

    let mut out = String::new();
    let mut lines = text.lines();
    let mut wrote_any = false;

    for (i, line) in lines.by_ref().take(OUTPUT_PREVIEW_MAX_LINES).enumerate() {
        if i > 0 {
            out.push('\n');
        }

        let mut chars = line.chars();
        out.extend(chars.by_ref().take(OUTPUT_PREVIEW_MAX_CHARS_PER_LINE));
        if chars.next().is_some() {
            out.push_str("...");
        }

        wrote_any = true;
    }

    if wrote_any {
        let more_lines = lines.next().is_some();
        if more_lines || decoded_incomplete {
            out.push_str("\n...");
        }
    } else if decoded_incomplete {
        out.push_str("...");
    }

    out
}
