fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(&["proto/oasis.proto"], &["proto/"])?;
    Ok(())
}
