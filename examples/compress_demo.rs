use filereducelib::{FileReduceCompressor, FileReduceDecompressor};
use std::io::Cursor;
// Removed Compressor trait import as inherent methods are used

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let jsonl_data = r#"
    {"id": 1, "event": "login", "timestamp": 1678886400}
    {"id": 2, "event": "purchase", "timestamp": 1678886500, "amount": 29.99}
    "#;

    println!("Original Data:\n{}", jsonl_data);

    // Compress
    let mut compressor = FileReduceCompressor::new();
    let mut compressed_output = Cursor::new(Vec::new()); // Must be Seek now
    compressor.compress(Cursor::new(jsonl_data), &mut compressed_output)?;

    let compressed_bytes = compressed_output.into_inner();

    println!("Compressed size: {} bytes", compressed_bytes.len());

    // Decompress
    let mut decompressor = FileReduceDecompressor::new();
    let mut decompressed_output = Vec::new(); // Writes to Write usually, but decompress method needs Seek input
                                              // The previous API was reader/writer. The NEW API mandates Seek for input Decompression (impl Decompressor)
                                              // Decompressor::decompress(&mut self, input: R, output: W). R: Read+Seek.

    let mut cursor_input = Cursor::new(compressed_bytes);
    decompressor.decompress(&mut cursor_input, &mut decompressed_output)?;

    let result_str = String::from_utf8(decompressed_output)?;
    println!("Decompressed Data:\n{}", result_str);

    Ok(())
}
