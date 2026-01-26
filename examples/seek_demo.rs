use filereducelib::{FileReduceCompressor, FileReduceDecompressor};
use serde_json::Value;
use std::io::Cursor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generando dataset de prueba (10,000 registros)...");
    let mut jsonl_input = String::new();
    for i in 0..10_000 {
        jsonl_input.push_str(&format!(
            r#"{{"id": {}, "type": "sensor_reading", "payload": {{"val": {}, "ts": 123456789}}}}"#,
            i,
            i * 10
        ));
        jsonl_input.push('\n');
    }

    // 1. Compress
    println!("Comprimiendo...");
    let mut compressor = FileReduceCompressor::new();
    let mut compressed_data = Cursor::new(Vec::new());
    compressor.compress(Cursor::new(&jsonl_input), &mut compressed_data)?;

    let compressed_bytes = compressed_data.into_inner();
    println!("Tamaño Comprimido: {} bytes", compressed_bytes.len());

    // 2. Random Access Test
    println!("Probando Acceso Aleatorio...");
    let mut decompressor = FileReduceDecompressor::new();
    let mut cursor = Cursor::new(compressed_bytes);

    // Seek to record 5000
    let target_id = 5000;
    let start_time = std::time::Instant::now();
    let record = decompressor.seek_record(&mut cursor, target_id)?;
    let duration = start_time.elapsed();

    println!("Registro #{} recuperado en {:?}:", target_id, duration);
    println!("{}", record);

    assert_eq!(record["id"], target_id);
    println!("¡Verificación exitosa!");

    Ok(())
}
