use std::io::{self, Read, Write};

use hashbrown::HashMap as HbHashMap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Error type for the library
#[derive(Error, Debug)]
pub enum FileReduceError {
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization Error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("JSON Parsing Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Decompression Error: {0}")]
    Decompression(String),
    #[error("Format Error: Invalid Magic or Version")]
    FormatError,
}

pub type Result<T> = std::result::Result<T, FileReduceError>;

/// Represents the simplified value types for our binary format.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BinaryValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Array(Vec<BinaryValue>),
    Object(Vec<(u16, BinaryValue)>), // Keys are compressed to u16 IDs
}

/// Internal packet types for the .fra stream
#[derive(Serialize, Deserialize, Debug)]
enum FraPacket {
    /// Define a new key mapping: ID -> String
    DefineKey(u16, String),
    /// A block of compressed records
    Block(Vec<BinaryValue>),
}

/// The Header of the .fra file
#[derive(Serialize, Deserialize, Debug)]
pub struct FraHeader {
    pub magic: [u8; 4],
    pub version: u8,
    // Initial dictionary could be here, but we use dynamic updates in the stream
    pub initial_dict_size: u32,
}

const MAGIC: [u8; 4] = [0x46, 0x52, 0x41, 0x01]; // "FRA" + 0x01

/// Main Compressor Struct
pub struct FileReduceCompressor {
    dict: HbHashMap<String, u16>,
    next_id: u16,
}

/// Main Decompressor Struct
pub struct FileReduceDecompressor {
    dict: HbHashMap<u16, String>,
}

pub trait Compressor {
    fn compress<R: Read, W: Write>(&mut self, input: R, output: W) -> Result<()>;
    fn decompress<R: Read, W: Write>(&mut self, input: R, output: W) -> Result<()>;
}

impl FileReduceCompressor {
    pub fn new() -> Self {
        Self {
            dict: HbHashMap::new(),
            next_id: 1, // Start IDs at 1
        }
    }

    /// Helper to convert standard JSON Value to our BinaryValue, updating dict as needed.
    fn process_chunk(&mut self, chunk: Vec<Value>) -> (Vec<FraPacket>, Vec<BinaryValue>) {
        let mut key_events = Vec::new();

        // First pass: Identify new keys sequentially
        let mut chunk_keys: Vec<String> = Vec::new();

        // Helper to recursively find keys
        fn extract_keys(v: &Value, keys: &mut Vec<String>) {
            match v {
                Value::Object(map) => {
                    for (k, v) in map {
                        keys.push(k.clone());
                        extract_keys(v, keys);
                    }
                }
                Value::Array(arr) => {
                    for v in arr {
                        extract_keys(v, keys);
                    }
                }
                _ => {}
            }
        }

        for record in &chunk {
            extract_keys(record, &mut chunk_keys);
        }

        // Dedup keys and assign IDs for new ones
        chunk_keys.sort();
        chunk_keys.dedup();

        for key in chunk_keys {
            if !self.dict.contains_key(&key) {
                let id = self.next_id;
                self.next_id += 1;
                self.dict.insert(key.clone(), id);
                key_events.push(FraPacket::DefineKey(id, key));
            }
        }

        // Second pass: Convert values using the (now updated) dict.
        // This is parallelized with Rayon.
        let dict_ref = &self.dict;

        let binaries: Vec<BinaryValue> = chunk
            .par_iter()
            .map(|v| Self::json_to_binary(v, dict_ref))
            .collect();

        (key_events, binaries)
    }

    fn json_to_binary(v: &Value, dict: &HbHashMap<String, u16>) -> BinaryValue {
        match v {
            Value::Null => BinaryValue::Null,
            Value::Bool(b) => BinaryValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    BinaryValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    BinaryValue::UInt(u)
                } else {
                    BinaryValue::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(s) => BinaryValue::String(s.clone()),
            Value::Array(arr) => {
                BinaryValue::Array(arr.iter().map(|v| Self::json_to_binary(v, dict)).collect())
            }
            Value::Object(map) => {
                let fields = map
                    .iter()
                    .map(|(k, v)| {
                        let id = dict.get(k).copied().unwrap_or(0); // Should be found
                        (id, Self::json_to_binary(v, dict))
                    })
                    .collect();
                BinaryValue::Object(fields)
            }
        }
    }
}

impl Compressor for FileReduceCompressor {
    fn compress<R: Read, W: Write>(&mut self, input: R, output: W) -> Result<()> {
        let mut writer = zstd::stream::write::Encoder::new(output, 3)?; // Level 3 default

        // Write Header
        let header = FraHeader {
            magic: MAGIC,
            version: 1,
            initial_dict_size: 0,
        };
        bincode::serialize_into(&mut writer, &header)?;

        // JSONL Iterator
        let deserializer = serde_json::Deserializer::from_reader(input);
        let iterator = deserializer.into_iter::<Value>();

        let mut buffer = Vec::with_capacity(1000);
        let chunk_size = 1000;

        for item in iterator {
            let val = item?;
            buffer.push(val);

            if buffer.len() >= chunk_size {
                let (packets, binaries) = self.process_chunk(buffer);

                // Write definitions
                for p in packets {
                    bincode::serialize_into(&mut writer, &p)?;
                }

                // Write data block
                bincode::serialize_into(&mut writer, &FraPacket::Block(binaries))?;

                buffer = Vec::with_capacity(chunk_size);
            }
        }

        // Flush remaining
        if !buffer.is_empty() {
            let (packets, binaries) = self.process_chunk(buffer);
            for p in packets {
                bincode::serialize_into(&mut writer, &p)?;
            }
            bincode::serialize_into(&mut writer, &FraPacket::Block(binaries))?;
        }

        writer.finish()?;
        Ok(())
    }

    fn decompress<R: Read, W: Write>(&mut self, _input: R, _output: W) -> Result<()> {
        Err(FileReduceError::Decompression(
            "Compressor cannot decompress. Use FileReduceDecompressor.".into(),
        ))
    }
}

impl FileReduceDecompressor {
    pub fn new() -> Self {
        Self {
            dict: HbHashMap::new(),
        }
    }

    fn binary_to_json(&self, val: &BinaryValue) -> Value {
        match val {
            BinaryValue::Null => Value::Null,
            BinaryValue::Bool(b) => Value::Bool(*b),
            BinaryValue::Int(i) => Value::Number((*i).into()),
            BinaryValue::UInt(u) => Value::Number((*u).into()),
            BinaryValue::Float(f) => serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            BinaryValue::String(s) => Value::String(s.clone()),
            BinaryValue::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.binary_to_json(v)).collect())
            }
            BinaryValue::Object(fields) => {
                let mut map = serde_json::Map::new();
                for (id, v) in fields {
                    if let Some(key) = self.dict.get(id) {
                        map.insert(key.clone(), self.binary_to_json(v));
                    }
                }
                Value::Object(map)
            }
        }
    }
}

impl Compressor for FileReduceDecompressor {
    fn compress<R: Read, W: Write>(&mut self, _input: R, _output: W) -> Result<()> {
        Err(FileReduceError::Decompression(
            "Decompressor cannot compress. Use FileReduceCompressor.".into(),
        ))
    }

    fn decompress<R: Read, W: Write>(&mut self, input: R, mut output: W) -> Result<()> {
        let mut reader = zstd::stream::read::Decoder::new(input)?;

        // Read Header
        let header: FraHeader = bincode::deserialize_from(&mut reader)?;
        if header.magic != MAGIC {
            return Err(FileReduceError::FormatError);
        }

        // Loop stream
        loop {
            // bincode doesn't have a clean 'peek', so we handle errors
            let packet_res: std::result::Result<FraPacket, bincode::Error> =
                bincode::deserialize_from(&mut reader);

            match packet_res {
                Ok(FraPacket::DefineKey(id, key)) => {
                    self.dict.insert(id, key);
                }
                Ok(FraPacket::Block(records)) => {
                    for record in records {
                        let json_val = self.binary_to_json(&record);
                        serde_json::to_writer(&mut output, &json_val)?;
                        output.write_all(b"\n")?;
                    }
                }
                Err(_) => break, // Assume EOF
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_compress_decompress_cycle() {
        let jsonl_input = r#"{"name": "Alice", "age": 30, "meta": {"score": 100}}
{"name": "Bob", "age": 25, "meta": {"score": 90}}
{"name": "Alice", "age": 31, "meta": {"score": 101}}"#;

        let mut compressor = FileReduceCompressor::new();
        let mut compressed_data = Vec::new();

        compressor
            .compress(Cursor::new(jsonl_input), &mut compressed_data)
            .expect("Compression failed");

        assert!(!compressed_data.is_empty());
        println!("Compressed size: {}", compressed_data.len());

        let mut decompressor = FileReduceDecompressor::new();
        let mut output_data = Vec::new();
        decompressor
            .decompress(Cursor::new(compressed_data), &mut output_data)
            .expect("Decompression failed");

        let output_str = String::from_utf8(output_data).expect("Invalid UTF8");
        // Verify output matches input logical structure (ignoring whitespace differences potentially)
        let input_lines: Vec<Value> = serde_json::Deserializer::from_str(jsonl_input)
            .into_iter::<Value>()
            .map(|x| x.unwrap())
            .collect();
        let output_lines: Vec<Value> = serde_json::Deserializer::from_str(&output_str)
            .into_iter::<Value>()
            .map(|x| x.unwrap())
            .collect();

        assert_eq!(input_lines, output_lines);
    }
}
