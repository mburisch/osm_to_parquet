use std::{
    fmt,
    fs::{File, create_dir_all},
    io::{BufWriter, Write},
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::arrow::ArrowWriter;

use crate::{
    osm::elements::{PrimitiveBlockDecoder, decode_nodes, decode_relations, decode_ways},
    osmpbf::PrimitiveBlock,
    parquet::{
        records::{convert_nodes, convert_relations, convert_ways},
        schemas::{get_node_schema, get_relation_schema, get_way_schema},
    },
};
use rayon;

#[derive(Debug, Clone, Default)]
pub struct WriterConfig {
    max_row_group_size: Option<usize>,
    max_rows_per_file: Option<usize>,
    max_file_size_bytes: Option<usize>,
    buffer_size: usize,
}

impl WriterConfig {
    pub fn new() -> Self {
        Self {
            max_row_group_size: None,
            max_rows_per_file: None,
            max_file_size_bytes: Some(128 * 1024 * 1024),
            buffer_size: 8 * 1024 * 1024,
        }
    }
}

pub fn create_parquet_writer<Target: Write + Send>(
    target: Target,
    schema: Arc<Schema>,
    writer_config: WriterConfig,
) -> ArrowWriter<Target> {
    let mut builder = parquet::file::properties::WriterProperties::builder();

    if let Some(max_row_group_size) = writer_config.max_row_group_size {
        builder = builder.set_max_row_group_size(max_row_group_size);
    }

    let props = builder.build();
    ArrowWriter::try_new(target, schema, Some(props)).unwrap()
}

#[derive(Debug, Clone, Default)]
pub struct WriteStatistics {
    pub nodes: usize,
    pub ways: usize,
    pub relations: usize,
}

impl WriteStatistics {
    pub fn total(&self) -> usize {
        self.nodes + self.ways + self.relations
    }
}

impl fmt::Display for WriteStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}, {})", self.nodes, self.ways, self.relations)
    }
}

pub struct ParquetMultiFileWriter {
    root_path: String,
    schema: Arc<Schema>,
    writer_config: WriterConfig,
    writer: Option<ArrowWriter<BufWriter<File>>>,
    file_index: usize,
    num_rows: usize,
}

impl ParquetMultiFileWriter {
    pub fn new(path: &str, schema: Arc<Schema>, writer_config: WriterConfig) -> Self {
        create_dir_all(path).unwrap();
        Self {
            root_path: path.to_string(),

            schema,
            writer_config,
            writer: None,
            file_index: 0,
            num_rows: 0,
        }
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn get_next_filename(&mut self) -> String {
        self.file_index += 1;
        let id = rayon::current_thread_index().unwrap() + 1;
        format!(
            "{}/data_{:0>4}_{:0>6}.parquet",
            self.root_path, id, self.file_index
        )
    }

    fn should_switch_writer(&self) -> bool {
        if self.writer.is_none() {
            return true;
        }
        let writer = self.writer.as_ref().unwrap();
        if let Some(max_file_size) = self.writer_config.max_file_size_bytes {
            if writer.bytes_written() + writer.in_progress_size() > max_file_size {
                return true;
            }
        }
        if let Some(max_rows_per_file) = self.writer_config.max_rows_per_file {
            if self.num_rows > max_rows_per_file {
                return true;
            }
        }
        false
    }

    fn change_writer(&mut self) {
        self.close();

        let f = File::create(&self.get_next_filename()).unwrap();
        let b = BufWriter::with_capacity(self.writer_config.buffer_size, f);
        self.writer = Some(create_parquet_writer(
            b,
            self.schema.clone(),
            self.writer_config.clone(),
        ));
    }

    pub fn write(&mut self, record: &RecordBatch) {
        if record.num_rows() == 0 {
            return;
        }

        if self.should_switch_writer() {
            self.change_writer();
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write(&record).unwrap();
        self.num_rows += record.num_rows();
    }

    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.close().unwrap();
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

pub struct OsmParquetWriter {
    nodes: ParquetMultiFileWriter,
    ways: ParquetMultiFileWriter,
    relations: ParquetMultiFileWriter,
}

impl OsmParquetWriter {
    pub fn new(
        root_path: &str,
        node_config: WriterConfig,
        way_config: WriterConfig,
        relation_config: WriterConfig,
    ) -> Self {
        Self {
            nodes: ParquetMultiFileWriter::new(
                &format!("{root_path}/nodes"),
                get_node_schema(),
                node_config,
            ),
            ways: ParquetMultiFileWriter::new(
                &format!("{root_path}/ways"),
                get_way_schema(),
                way_config,
            ),
            relations: ParquetMultiFileWriter::new(
                &format!("{root_path}/relations"),
                get_relation_schema(),
                relation_config,
            ),
        }
    }

    pub fn stats(&self) -> WriteStatistics {
        return WriteStatistics {
            nodes: self.nodes.num_rows(),
            ways: self.ways.num_rows(),
            relations: self.relations.num_rows(),
        };
    }

    pub fn write_elements(&mut self, block: &PrimitiveBlock) -> WriteStatistics {
        let decoder = PrimitiveBlockDecoder::new(&block);

        let nodes = decode_nodes(&block, &decoder);
        if !nodes.is_empty() {
            self.nodes
                .write(&convert_nodes(&nodes, self.nodes.schema()));
        }

        let ways = decode_ways(&block, &decoder);
        if !ways.is_empty() {
            self.ways.write(&convert_ways(&ways, self.ways.schema()));
        }

        let relations = decode_relations(&block, &decoder);
        if !relations.is_empty() {
            self.relations
                .write(&convert_relations(&relations, self.relations.schema()));
        }

        WriteStatistics {
            nodes: nodes.len(),
            ways: ways.len(),
            relations: relations.len(),
        }
    }

    pub fn close(&mut self) {
        self.nodes.close();
        self.ways.close();
        self.relations.close();
    }
}

impl Drop for OsmParquetWriter {
    fn drop(&mut self) {
        self.close();
    }
}
