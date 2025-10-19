use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use parquet::arrow::ArrowWriter;

pub fn create_parquet_writer<Target: Write + Send>(
    target: Target,
    schema: Arc<Schema>,
) -> ArrowWriter<Target> {
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(1024 * 1024)
        .build();

    ArrowWriter::try_new(target, schema, Some(props)).unwrap()
}

pub fn create_parquet_writer_for_filename(
    filename: &str,
    schema: Arc<Schema>,
) -> ArrowWriter<BufWriter<File>> {
    let f = File::create(filename).unwrap();
    let b = BufWriter::with_capacity(8 * 1024 * 1024, f);
    ArrowWriter::try_new(b, schema, None).unwrap()
}

#[derive(Debug, Clone)]
pub struct ParquetFilenameGenerator {
    path: String,
    element_type: String,
    generator_index: i32,
    file_index: i32,
}

impl ParquetFilenameGenerator {
    pub fn new(path: &str, element_type: &str, generator_index: i32) -> Self {
        Self {
            path: path.to_string(),
            element_type: element_type.to_string(),
            generator_index,
            file_index: 0,
        }
    }

    fn get_next_filename(&mut self) -> String {
        self.file_index += 1;
        format!(
            "{}/{}_{:02}_{:06}.parquet",
            self.path, self.element_type, self.generator_index, self.file_index
        )
    }
}

pub struct ParquetWriter {
    writer: Option<ArrowWriter<BufWriter<File>>>,
    schema: Arc<Schema>,
    filename_getter: ParquetFilenameGenerator,
    max_file_size: usize,
}

impl ParquetWriter {
    pub fn new(
        schema: Arc<Schema>,
        filename_getter: ParquetFilenameGenerator,
        max_file_size: usize,
    ) -> Self {
        Self {
            writer: None,
            schema,
            filename_getter,
            max_file_size: max_file_size,
        }
    }

    fn must_change_writer(&self) -> bool {
        if self.writer.is_none() {
            return true;
        }
        let writer = self.writer.as_ref().unwrap();
        if self.max_file_size > 0 {
            if writer.bytes_written() + writer.in_progress_size() > self.max_file_size {
                return true;
            }
        }
        false
    }

    fn change_writer(&mut self) {
        self.close();

        let filename = self.filename_getter.get_next_filename();
        self.writer = Some(create_parquet_writer_for_filename(
            &filename,
            self.schema.clone(),
        ));
    }

    pub fn writer(&mut self) -> &ArrowWriter<BufWriter<File>> {
        if self.must_change_writer() {
            self.change_writer();
        }
        self.writer.as_ref().unwrap()
    }

    pub fn write(&mut self, record: &RecordBatch) {
        if self.must_change_writer() {
            self.change_writer();
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write(&record).unwrap();
    }

    pub fn flush(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().unwrap();
        }
    }

    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.close().unwrap();
        }
    }
}
