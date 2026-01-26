use filereducelib::{FileReduceCompressor, FileReduceDecompressor};
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = "outputfull.jsonl";
    let compressed_path = "outputfull.fra";
    let decompressed_path = "outputfull_recovered.jsonl";

    println!("Iniciando prueba de Stress (Enterprise V2)...");
    println!("Entrada: {}", input_path);

    // 1. Compress
    println!("1. Comprimiendo...");
    let start_compress = Instant::now();
    let mut record_count = 0;
    {
        let input_file = File::open(input_path)?;
        let mut reader = BufReader::new(input_file);

        // We write to BufWriter for speed
        let output_file = File::create(compressed_path)?;
        let mut writer = BufWriter::new(output_file);

        let mut compressor = FileReduceCompressor::new();
        compressor.compress(&mut reader, &mut writer)?;
        // Ideally we'd like to know how many records, but the API doesn't return it yet.
    }
    let duration_compress = start_compress.elapsed();

    let input_metadata = std::fs::metadata(input_path)?;
    let compressed_metadata = std::fs::metadata(compressed_path)?;

    println!("   Tiempo: {:.2?}", duration_compress);
    println!("   Tamaño Original: {} bytes", input_metadata.len());
    println!("   Tamaño Comprimido: {} bytes", compressed_metadata.len());
    if input_metadata.len() > 0 {
        println!(
            "   Ratio de Compresión: {:.2}%",
            (compressed_metadata.len() as f64 / input_metadata.len() as f64) * 100.0
        );
    }

    // 2. Decompress
    println!("\n2. Descomprimiendo...");
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

    println!("   Tiempo: {:.2?}", duration_decompress);
    println!("   Tamaño Recuperado: {} bytes", recovered_metadata.len());

    // 3. Random Access Test
    println!("\n3. Prueba de Acceso Aleatorio (Seek)...");
    let start_seek = Instant::now();
    {
        let input_file = File::open(compressed_path)?;
        let mut reader = BufReader::new(input_file);
        let mut decompressor = FileReduceDecompressor::new();

        // Try to verify quick access to deep records
        // We pick arbitrary IDs assuming the file has many records.
        // If file is 350MB, likely > 1M records.
        let targets = vec![0, 1000, 500_000, 1_000_000];

        for &id in &targets {
            let t0 = Instant::now();
            match decompressor.seek_record(&mut reader, id) {
                Ok(val) => {
                    println!(
                        "   [OK] Registro #{:<7} encontrado en {:?} | Preview: {:.50}...",
                        id,
                        t0.elapsed(),
                        val.to_string()
                    );
                }
                Err(e) => {
                    println!(
                        "   [Info] Registro #{:<7} no accesible (posiblemente fuera de rango): {}",
                        id, e
                    );
                }
            }
        }
    }
    println!("   Tiempo total pruebas seek: {:.2?}", start_seek.elapsed());

    Ok(())
}
