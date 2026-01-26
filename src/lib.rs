use std::io::{self, Read, Seek, SeekFrom, Write};

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
    #[error("Seek Error: Record not found")]
    RecordNotFound,
}

pub type Result<T> = std::result::Result<T, FileReduceError>;

const MAGIC: [u8; 4] = [0x46, 0x52, 0x41, 0x02]; // "FRA" + 0x02 (Version 2)
const CHUNK_SIZE: usize = 1000; // Records per block

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

/// A block of compressed records.
/// In V2, we only serialize this, compress it, and write it as a frame.
#[derive(Serialize, Deserialize, Debug)]
struct BlockData {
    records: Vec<BinaryValue>,
}

/// The Header of the .fra file (Start of file)
#[derive(Serialize, Deserialize, Debug)]
pub struct FraHeader {
    pub magic: [u8; 4],
    pub version: u8,
}

/// The Index Map entry
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexEntry {
    pub start_record_id: u64, // The global record ID (0-indexed) this block starts with
    pub byte_offset: u64,     // The absolute byte offset in the file where the block frame starts
}

/// The Footer of the .fra file (End of file)
/// Contains the complete Dictionary and the Index for Random Access.
#[derive(Serialize, Deserialize, Debug)]
pub struct FraFooter {
    pub final_dictionary: HbHashMap<u16, String>,
    pub index: Vec<IndexEntry>,
    pub total_records: u64,
}

/// Main Compressor Struct
pub struct FileReduceCompressor {
    dict: HbHashMap<String, u16>,
    next_id: u16,
    index: Vec<IndexEntry>,
    total_records: u64,
}

/// Main Decompressor Struct
pub struct FileReduceDecompressor {
    dict: HbHashMap<u16, String>,
    index: Vec<IndexEntry>,
    total_records: u64,
}

pub trait Compressor {
    fn compress<W: Write + Seek>(&mut self, input: impl Read, output: W) -> Result<()>;
}

pub trait Decompressor {
    fn decompress<R: Read + Seek, W: Write>(&mut self, input: R, output: W) -> Result<()>;
    fn seek_record<R: Read + Seek>(&mut self, input: &mut R, record_id: u64) -> Result<Value>;
}

impl FileReduceCompressor {
    pub fn new() -> Self {
        Self {
            dict: HbHashMap::new(),
            next_id: 1, // Start IDs at 1
            index: Vec::new(),
            total_records: 0,
        }
    }

    /// Process a chunk of JSON Values into BinaryValues and update local dictionary state.
    fn process_chunk(&mut self, chunk: Vec<Value>) -> Result<Vec<u8>> {
        // 1. Scan for Keys (Sequential or Parallel Reduce)
        let mut keys_to_add = Vec::new();

        // Use a simple scanner for the chunk
        for record in &chunk {
            self.scan_keys(record, &mut keys_to_add);
        }

        // Update Dictionary
        for key in keys_to_add {
            if !self.dict.contains_key(&key) {
                self.dict.insert(key, self.next_id);
                self.next_id += 1;
            }
        }

        // 2. Convert to Binary (Parallel)
        // We need a read-only view of the dict for Rayon
        let dict_ref = &self.dict;
        let binaries: Vec<BinaryValue> = chunk
            .par_iter()
            .map(|v| Self::json_to_binary(v, dict_ref))
            .collect();

        // 3. Serialize BlockData
        let block = BlockData { records: binaries };
        let serialized_block = bincode::serialize(&block)?;

        // 4. Compress Independently (Zstd Frame)
        let compressed_block = zstd::bulk::compress(&serialized_block, 3)?;

        Ok(compressed_block)
    }

    fn scan_keys(&self, v: &Value, sink: &mut Vec<String>) {
        match v {
            Value::Object(map) => {
                for (k, v) in map {
                    if !self.dict.contains_key(k) {
                        sink.push(k.clone());
                    }
                    self.scan_keys(v, sink);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    self.scan_keys(v, sink);
                }
            }
            _ => {}
        }
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
                        let id = dict.get(k).copied().unwrap_or(0);
                        (id, Self::json_to_binary(v, dict))
                    })
                    .collect();
                BinaryValue::Object(fields)
            }
        }
    }
}

// Inherent methods to maintain compatibility and ease of use
impl FileReduceCompressor {
    pub fn compress<W: Write + Seek>(&mut self, input: impl Read, output: W) -> Result<()> {
        <Self as Compressor>::compress(self, input, output)
    }
}

impl Compressor for FileReduceCompressor {
    fn compress<W: Write + Seek>(&mut self, input: impl Read, mut output: W) -> Result<()> {
        // 1. Write Header
        let header = FraHeader {
            magic: MAGIC,
            version: 2,
        };
        bincode::serialize_into(&mut output, &header)?;
        let mut current_offset = output.stream_position()?;

        // 2. Stream Processing
        let deserializer = serde_json::Deserializer::from_reader(input);
        let iterator = deserializer.into_iter::<Value>();

        let mut chunk_buffer = Vec::with_capacity(CHUNK_SIZE);

        for item in iterator {
            let val = item?;
            chunk_buffer.push(val);

            if chunk_buffer.len() >= CHUNK_SIZE {
                // Process Chunk
                let compressed_data = self.process_chunk(chunk_buffer)?;
                let compressed_len = compressed_data.len() as u32;

                // Record Index
                self.index.push(IndexEntry {
                    start_record_id: self.total_records,
                    byte_offset: current_offset,
                });
                self.total_records += CHUNK_SIZE as u64;

                // Write Frame: [Length u32] [Data]
                output.write_all(&compressed_len.to_le_bytes())?;
                output.write_all(&compressed_data)?;

                current_offset = output.stream_position()?;
                chunk_buffer = Vec::with_capacity(CHUNK_SIZE);
            }
        }

        // Flush remaining
        if !chunk_buffer.is_empty() {
            let processed_count = chunk_buffer.len() as u64;
            let compressed_data = self.process_chunk(chunk_buffer)?;
            let compressed_len = compressed_data.len() as u32;

            self.index.push(IndexEntry {
                start_record_id: self.total_records,
                byte_offset: current_offset,
            });
            self.total_records += processed_count;

            output.write_all(&compressed_len.to_le_bytes())?;
            output.write_all(&compressed_data)?;
            current_offset = output.stream_position()?;
        }

        // 3. Write Footer
        let footer_start = current_offset;

        let mut final_dict_rev = HbHashMap::new();
        for (k, v) in &self.dict {
            final_dict_rev.insert(*v, k.clone());
        }

        let footer = FraFooter {
            final_dictionary: final_dict_rev,
            index: self.index.clone(),
            total_records: self.total_records,
        };

        let footer_bytes = bincode::serialize(&footer)?;
        output.write_all(&footer_bytes)?;

        // 4. Write Footer Metadata
        // [Footer Start Offset u64] [Magic u32]
        output.write_all(&footer_start.to_le_bytes())?;
        output.write_all(&MAGIC)?;

        Ok(())
    }
}

// Inherent methods for Decompressor
impl FileReduceDecompressor {
    pub fn new() -> Self {
        Self {
            dict: HbHashMap::new(),
            index: Vec::new(),
            total_records: 0,
        }
    }

    pub fn decompress<R: Read + Seek, W: Write>(&mut self, input: R, output: W) -> Result<()> {
        <Self as Decompressor>::decompress(self, input, output)
    }

    pub fn seek_record<R: Read + Seek>(&mut self, input: &mut R, record_id: u64) -> Result<Value> {
        <Self as Decompressor>::seek_record(self, input, record_id)
    }

    fn read_metadata<R: Read + Seek>(&mut self, reader: &mut R) -> Result<u64> {
        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < 16 {
            return Err(FileReduceError::FormatError);
        }

        reader.seek(SeekFrom::End(-12))?;

        let mut buf_u64 = [0u8; 8];
        let mut buf_magic = [0u8; 4];

        reader.read_exact(&mut buf_u64)?;
        reader.read_exact(&mut buf_magic)?;

        if buf_magic != MAGIC {
            return Err(FileReduceError::FormatError);
        }

        let footer_start = u64::from_le_bytes(buf_u64);

        reader.seek(SeekFrom::Start(footer_start))?;
        let footer: FraFooter = bincode::deserialize_from(reader)?;

        self.dict = footer.final_dictionary;
        self.index = footer.index;
        self.total_records = footer.total_records;

        Ok(footer_start)
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

impl Decompressor for FileReduceDecompressor {
    fn decompress<R: Read + Seek, W: Write>(&mut self, mut input: R, mut output: W) -> Result<()> {
        let footer_start = self.read_metadata(&mut input)?;

        input.seek(SeekFrom::Start(0))?;
        let header: FraHeader = bincode::deserialize_from(&mut input)?;
        if header.magic != MAGIC {
            return Err(FileReduceError::FormatError);
        }

        let mut current_pos = input.stream_position()?;

        while current_pos < footer_start {
            let mut len_buf = [0u8; 4];
            input.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut compressed = vec![0u8; len];
            input.read_exact(&mut compressed)?;

            let decompressed = zstd::bulk::decompress(&compressed, 10_000_000)
                .map_err(|e| FileReduceError::Decompression(e.to_string()))?;

            let block: BlockData = bincode::deserialize(&decompressed)?;

            for record in block.records {
                let v = self.binary_to_json(&record);
                serde_json::to_writer(&mut output, &v)?;
                output.write_all(b"\n")?;
            }

            current_pos = input.stream_position()?;
        }

        Ok(())
    }

    fn seek_record<R: Read + Seek>(&mut self, input: &mut R, record_id: u64) -> Result<Value> {
        if self.index.is_empty() {
            self.read_metadata(input)?;
        }

        if record_id >= self.total_records {
            return Err(FileReduceError::RecordNotFound);
        }

        let block_idx = match self
            .index
            .binary_search_by_key(&record_id, |entry| entry.start_record_id)
        {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };

        let entry = &self.index[block_idx];

        input.seek(SeekFrom::Start(entry.byte_offset))?;

        let mut len_buf = [0u8; 4];
        input.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut compressed = vec![0u8; len];
        input.read_exact(&mut compressed)?;

        let decompressed = zstd::bulk::decompress(&compressed, 10_000_000)
            .map_err(|e| FileReduceError::Decompression(e.to_string()))?;

        let block: BlockData = bincode::deserialize(&decompressed)?;

        let relative_index = (record_id - entry.start_record_id) as usize;
        if relative_index >= block.records.len() {
            return Err(FileReduceError::RecordNotFound);
        }

        let record_bin = &block.records[relative_index];
        Ok(self.binary_to_json(record_bin))
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
        let mut compressed_data = Cursor::new(Vec::new());

        compressor
            .compress(Cursor::new(jsonl_input), &mut compressed_data)
            .expect("Compression failed");

        let compressed_vec = compressed_data.into_inner();
        assert!(!compressed_vec.is_empty());
        println!("Compressed size: {}", compressed_vec.len());

        let mut decompressor = FileReduceDecompressor::new();
        let mut output_data = Vec::new();
        decompressor
            .decompress(Cursor::new(compressed_vec), &mut output_data)
            .expect("Decompression failed");

        let output_str = String::from_utf8(output_data).expect("Invalid UTF8");

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

    #[test]
    fn test_seek_random_access() {
        let mut jsonl_input = String::new();
        for i in 0..5000 {
            jsonl_input.push_str(&format!(r#"{{"id": {}, "data": "record_{}"}}"#, i, i));
            jsonl_input.push('\n');
        }

        let mut compressor = FileReduceCompressor::new();
        let mut compressed_data = Cursor::new(Vec::new());
        compressor
            .compress(Cursor::new(&jsonl_input), &mut compressed_data)
            .expect("Compression failed");

        let compressed_vec = compressed_data.into_inner();
        let mut decompressor = FileReduceDecompressor::new();
        let mut cursor = Cursor::new(compressed_vec);

        // Seek Record 2500 (middle)
        let record_val = decompressor
            .seek_record(&mut cursor, 2500)
            .expect("Seek failed");
        assert_eq!(record_val["id"], 2500);
        assert_eq!(record_val["data"], "record_2500");

        // Seek Record 0
        let record_val = decompressor
            .seek_record(&mut cursor, 0)
            .expect("Seek failed");
        assert_eq!(record_val["id"], 0);

        // Seek Last Record
        let record_val = decompressor
            .seek_record(&mut cursor, 4999)
            .expect("Seek failed");
        assert_eq!(record_val["id"], 4999);
    }
}
