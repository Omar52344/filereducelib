use filereducelib::{Compressor, FileReduceCompressor, FileReduceDecompressor};
use std::io::Cursor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl_data = r#"
    {"id": 1, "event": "login", "timestamp": 1678886400}
    {"id": 2, "event": "purchase", "timestamp": 1678886500, "amount": 29.99}
    "#;

    println!("Original Data:\n{}", jsonl_data);

    // Compress
    let mut compressor = FileReduceCompressor::new();
    let mut compressed_output = Vec::new();
    compressor.compress(Cursor::new(jsonl_data), &mut compressed_output)?;

    println!("Compressed size: {} bytes", compressed_output.len());

    // Decompress
    let mut decompressor = FileReduceDecompressor::new();
    let mut decompressed_output = Vec::new();
    decompressor.decompress(Cursor::new(compressed_output), &mut decompressed_output)?;

    let result_str = String::from_utf8(decompressed_output)?;
    println!("Decompressed Data:\n{}", result_str);

    Ok(())
}
