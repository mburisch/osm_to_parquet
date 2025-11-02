use bytes::Bytes;
use std::{
    fs::File,
    io::{self, BufWriter, Result, Write},
    path::PathBuf,
    sync::Arc,
    usize,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::parquet::records::ElementBatches;

#[derive(Debug)]
pub enum ParquetData {
    Node(Bytes),
    Way(Bytes),
    Relation(Bytes),
}

pub fn create_writer_options() -> WriterProperties {
    let props = WriterProperties::builder();
    props.set_compression(Compression::SNAPPY).build()
}

pub fn create_parquet_writer<Target: Write + Send>(
    target: Target,
    schema: Arc<Schema>,
    props: Option<WriterProperties>,
) -> Result<ArrowWriter<Target>> {
    ArrowWriter::try_new(target, schema, props).map_err(io::Error::from)
}

pub fn create_parquet_file_writer(
    path: &PathBuf,
    schema: Arc<Schema>,
    props: Option<WriterProperties>,
) -> Result<ArrowWriter<BufWriter<File>>> {
    let f = File::create(path)?;
    let buf = BufWriter::with_capacity(128 * 1024 * 1024, f);
    create_parquet_writer(buf, schema, props)
}

pub fn create_parquet_memory_writer(
    schema: Arc<Schema>,
    props: Option<WriterProperties>,
) -> Result<ArrowWriter<Vec<u8>>> {
    let buf = Vec::new();
    create_parquet_writer(buf, schema, props)
}

#[derive(Debug, Clone, Default)]
pub struct ParquetFileConfig {
    max_rows_per_file: Option<usize>,
    max_file_size_bytes: Option<usize>,
}

impl ParquetFileConfig {
    pub fn new() -> Self {
        Self {
            max_rows_per_file: None,
            max_file_size_bytes: Some(128 * 1024 * 1024),
        }
    }
}

pub trait ParquetStreamWriter {
    fn schema(&self) -> Arc<Schema>;
    fn write(&mut self, record: &RecordBatch) -> Result<()>;
    fn num_rows(&self) -> usize;
    fn num_bytes(&self) -> usize;
    fn should_flush(&self) -> bool;
    fn flush(&mut self) -> Result<Option<Bytes>>;
}

pub struct ParquetMemoryStreamWriter {
    schema: Arc<Schema>,
    props: Option<WriterProperties>,
    config: ParquetFileConfig,
    writer: Option<ArrowWriter<Vec<u8>>>,
    num_rows: usize,
}

impl ParquetMemoryStreamWriter {
    pub fn new(
        schema: Arc<Schema>,
        props: Option<WriterProperties>,
        config: Option<ParquetFileConfig>,
    ) -> Self {
        Self {
            schema: schema,
            props: props.or(Some(create_writer_options())),
            config: config.unwrap_or(ParquetFileConfig::new()),
            writer: None,
            num_rows: 0,
        }
    }
}

impl ParquetStreamWriter for ParquetMemoryStreamWriter {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn write(&mut self, record: &RecordBatch) -> Result<()> {
        if self.writer.is_none() {
            self.writer = Some(create_parquet_memory_writer(
                self.schema.clone(),
                self.props.clone(),
            )?);
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write(&record).unwrap();
        self.num_rows += record.num_rows();
        Ok(())
    }

    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn num_bytes(&self) -> usize {
        self.writer.as_ref().unwrap().bytes_written()
            + self.writer.as_ref().unwrap().in_progress_size()
    }

    fn should_flush(&self) -> bool {
        if let Some(writer) = self.writer.as_ref() {
            if let Some(max_file_size) = self.config.max_file_size_bytes {
                if writer.bytes_written() + writer.in_progress_size() > max_file_size {
                    return true;
                }
            }
            if let Some(max_rows_per_file) = self.config.max_rows_per_file {
                if self.num_rows > max_rows_per_file {
                    return true;
                }
            }
        }
        false
    }

    fn flush(&mut self) -> Result<Option<Bytes>> {
        if let Some(writer) = self.writer.take() {
            let bytes = writer.into_inner()?;
            self.num_rows = 0;

            if bytes.is_empty() {
                return Ok(None);
            }
            return Ok(Some(Bytes::from(bytes)));
        }
        return Ok(None);
    }
}

pub struct OsmParquetStreamWriter {
    nodes: Box<dyn ParquetStreamWriter>,
    ways: Box<dyn ParquetStreamWriter>,
    relations: Box<dyn ParquetStreamWriter>,
}

impl OsmParquetStreamWriter {
    pub fn new(
        nodes: Box<dyn ParquetStreamWriter>,
        ways: Box<dyn ParquetStreamWriter>,
        relations: Box<dyn ParquetStreamWriter>,
    ) -> Self {
        Self {
            nodes: nodes,
            ways: ways,
            relations: relations,
        }
    }

    pub fn write(&mut self, elements: &ElementBatches) -> Result<()> {
        if let Some(nodes) = elements.nodes.as_ref() {
            self.nodes.write(nodes)?;
        }
        if let Some(ways) = elements.ways.as_ref() {
            self.ways.write(ways)?;
        }
        if let Some(relations) = elements.relations.as_ref() {
            self.relations.write(relations)?;
        }
        Ok(())
    }

    pub fn flush(&mut self, force: bool) -> Result<Vec<ParquetData>> {
        let mut data = Vec::new();
        if force || self.nodes.should_flush() {
            if let Some(nodes) = self.nodes.flush()? {
                data.push(ParquetData::Node(nodes));
            }
        }
        if force || self.ways.should_flush() {
            if let Some(ways) = self.ways.flush()? {
                data.push(ParquetData::Way(ways));
            }
        }
        if force || self.relations.should_flush() {
            if let Some(relations) = self.relations.flush()? {
                data.push(ParquetData::Relation(relations));
            }
        }
        Ok(data)
    }
}
