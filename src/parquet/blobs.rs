use std::io::{self, Result};
use std::sync::{Arc, OnceLock};

use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;

use crate::osm::blobs::RawBlob;
use crate::parquet::writer::create_parquet_memory_writer;

pub fn create_blob_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new("blob_index", DataType::Int64, false),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("blob_data", DataType::Binary, false),
    ];
    Arc::new(Schema::new(fields))
}

pub fn get_blob_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    SCHEMA.get_or_init(create_blob_schema).clone()
}

#[derive(Debug, Clone, Copy)]
pub struct BlobParquetConfig {
    pub max_blobs_per_file: Option<usize>,
    pub max_file_size_bytes: Option<usize>,
}

impl Default for BlobParquetConfig {
    fn default() -> Self {
        Self {
            max_blobs_per_file: None,
            max_file_size_bytes: Some(128 * 1024 * 1024),
        }
    }
}

pub struct BlobParquetAccumulator {
    indices: Int64Builder,
    types: StringBuilder,
    data: BinaryBuilder,
    num_rows: usize,
    data_bytes: usize,
    config: BlobParquetConfig,
}

impl BlobParquetAccumulator {
    pub fn new(config: BlobParquetConfig) -> Self {
        Self {
            indices: Int64Builder::new(),
            types: StringBuilder::new(),
            data: BinaryBuilder::new(),
            num_rows: 0,
            data_bytes: 0,
            config,
        }
    }

    pub fn append(&mut self, index: i64, blob: &RawBlob) {
        self.indices.append_value(index);
        self.types.append_value(&blob.blob_type);
        self.data.append_value(&blob.data);
        self.num_rows += 1;
        self.data_bytes += blob.data.len();
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn should_flush(&self) -> bool {
        if self.num_rows == 0 {
            return false;
        }
        if let Some(max) = self.config.max_blobs_per_file {
            if self.num_rows >= max {
                return true;
            }
        }
        if let Some(max) = self.config.max_file_size_bytes {
            if self.data_bytes >= max {
                return true;
            }
        }
        false
    }

    pub fn flush(&mut self) -> Result<Option<Bytes>> {
        if self.num_rows == 0 {
            return Ok(None);
        }

        let schema = get_blob_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.indices.finish()) as ArrayRef,
            Arc::new(self.types.finish()) as ArrayRef,
            Arc::new(self.data.finish()) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut writer = create_parquet_memory_writer(schema, None)?;
        writer
            .write(&batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let bytes = writer.into_inner()?;

        self.num_rows = 0;
        self.data_bytes = 0;

        Ok(Some(Bytes::from(bytes)))
    }
}
