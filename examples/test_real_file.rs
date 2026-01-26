use filereducelib::{Compressor, FileReduceCompressor, FileReduceDecompressor};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = "outputfull.jsonl";
    let compressed_path = "outputfull.fra";
    let decompressed_path = "outputfull_recovered.jsonl";

    println!("Iniciando prueba de compresión...");
    println!("Entrada: {}", input_path);

    // 1. Compress
    let start_compress = Instant::now();
    {
        let input_file = File::open(input_path)?;
        let mut reader = BufReader::new(input_file);
        let output_file = File::create(compressed_path)?;
        let mut writer = BufWriter::new(output_file);

        let mut compressor = FileReduceCompressor::new();
        compressor.compress(&mut reader, &mut writer)?;
    }
    let duration_compress = start_compress.elapsed();

    let input_metadata = std::fs::metadata(input_path)?;
    let compressed_metadata = std::fs::metadata(compressed_path)?;

    println!("Compresión completada en {:.2?}", duration_compress);
    println!("Tamaño Original: {} bytes", input_metadata.len());
    println!("Tamaño Comprimido: {} bytes", compressed_metadata.len());
    println!(
        "Ratio de Compresión: {:.2}%",
        (compressed_metadata.len() as f64 / input_metadata.len() as f64) * 100.0
    );

    // 2. Decompress
    println!("\nIniciando prueba de descompresión...");
    let start_decompress = Instant::now();
    {
        let input_file = File::open(compressed_path)?;
        let mut reader = BufReader::new(input_file);
        let output_file = File::create(decompressed_path)?;
        let mut writer = BufWriter::new(output_file);

        let mut decompressor = FileReduceDecompressor::new();
        decompressor.decompress(&mut reader, &mut writer)?;
    }
    let duration_decompress = start_decompress.elapsed();
    let recovered_metadata = std::fs::metadata(decompressed_path)?;

    println!("Descompresión completada en {:.2?}", duration_decompress);
    println!("Tamaño Recuperado: {} bytes", recovered_metadata.len());
    println!("Archivo recuperado guardado en: {}", decompressed_path);

    Ok(())
}
