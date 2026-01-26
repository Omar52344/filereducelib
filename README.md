# FileReduceLib (.fra) 🚀
> **Compresión Semántica de Alto Rendimiento para Big Data (JSONL)**

[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Performance](https://img.shields.io/badge/performance-Blazing%20Fast-red.svg)]()
[![Compression](https://img.shields.io/badge/compression-0.6%25-green.svg)]()

**FileReduceLib** es una librería de Rust diseñada para la era del Big Data. Su misión es simple: **Reducir gigabytes de logs y datos estructurados (JSONL) a megabytes**, permitiendo acceso aleatorio instantáneo y operando con una huella de memoria constante.

Genera archivos **.fra** (FileReduce Archive), un formato propietario optimizado para superar a ZIP/RAR/GZIP en datos estructurados repetitivos.

---

## ⚡ ¿Por qué FileReduce?

| Característica | Descripción |
|----------------|-------------|
| **Compresión Extrema** | Ratios de compresión de hasta **0.6%** (200x más pequeño). Convierte 350MB en ~2MB. |
| **Memoria Constante** | Procesa archivos de **100GB+** usando solo **~100MB de RAM**. Arquitectura Streaming & Chunking. |
| **Acceso Aleatorio (Seek)** | ¿Necesitas el registro #1,000,000? Accédelo en **milisegundos** sin descomprimir todo el archivo. |
| **Diccionario Dinámico** | Aprende la estructura de tus datos "al vuelo". Reemplaza keys largas (`"transaction_timestamp"`) por tokens binarios (`0x01`). |
| **Enterprise Grade** | Diseñado para sistemas de baja latencia. Zero-copy deserialization, Rayon parallelism y Zstd framing. |

---

## 📦 Instalación

Añade `filereducelib` a tu `Cargo.toml`:

```toml
[dependencies]
filereducelib = { git = "https://github.com/Omar52344/filereducelib" }
```

---

## 🚀 Uso Rápido

### Comprimir un Archivo (Stream)

```rust
use std::fs::File;
use std::io::{BufReader, BufWriter};
use filereducelib::FileReduceCompressor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = File::open("data.jsonl")?;
    let output = File::create("data.fra")?;
    
    let mut reader = BufReader::new(input);
    let mut writer = BufWriter::new(output);

    let mut compressor = FileReduceCompressor::new();
    compressor.compress(&mut reader, &mut writer)?;
    
    println!("¡Compresión completada!");
    Ok(())
}
```

### Leer un Registro Específico (Random Access)

```rust
use std::fs::File;
use std::io::{BufReader, Seek};
use filereducelib::FileReduceDecompressor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = File::open("data.fra")?;
    let mut reader = BufReader::new(input);
    
    let mut decompressor = FileReduceDecompressor::new();
    
    // Saltamos directamente al registro 500,000
    let record = decompressor.seek_record(&mut reader, 500_000)?;
    
    println!("Registro encontrado: {}", record);
    Ok(())
}
```

---

## 🏗 Arquitectura del Formato .fra

El formato **.fra** (Versión 2) está diseñado para ser robusto y navegable:

1.  **Header**: Firma mágica `FRA\x02` y versión.
2.  **Payload (Chunks)**:
    *   Los datos se dividen en bloques de 1,000 registros.
    *   Cada bloque se serializa a binario (eliminando `{`, `}`, `"`, `:`) y se comprime como un **Zstd Frame independiente**.
    *   Esto permite descomprimir chunks individuales sin leer el archivo previo.
3.  **Footer Index**:
    *   Al final del archivo se escribe un Índice Maestro `[Record ID -> Byte Offset]`.
    *   Permite búsqueda binaria O(log n) para localizar cualquier registro.
4.  **Diccionario Global**: Se guarda al final para reconstruir las claves originales.

---

## 📊 Benchmarks

Prueba realizada en un archivo **JSONL real de 350 MB**:

| Métrica | Valor |
|---------|-------|
| Tamaño Original | **350,000,000 bytes** |
| Tamaño Comprimido (.fra) | **2,119,972 bytes** |
| Ratio de Compresión | **0.61%** (165x) |
| Tiempo de Compresión | ~7.15s |
| Tiempo de Búsqueda (Seek) | **~4ms** |

> *Nota: Los resultados pueden variar según la entropía y estructura de tus datos.*

---

## 🤝 Contribuyendo

¡Necesitamos tu ayuda para hacer esto aún más rápido!

1.  Haz un **Fork** del repositorio.
2.  Crea tu rama de feature (`git checkout -b feature/AmazingOptimization`).
3.  Asegúrate de que los tests pasen (`cargo test`).
4.  Abre un **Pull Request**.

### Áreas de interés para colaborar:
*   [ ] Soporte para compresión de tipos de datos específicos (DateTimes, IPs, UUIDs).
*   [ ] Implementación de `Async` (Tokio/Async-std).
*   [ ] CLI Tool oficial (`cargo install filereduce-cli`).

---

## 📄 Licencia

Este proyecto está bajo la Licencia **MIT**. Eres libre de usarlo, modificarlo y distribuirlo.
