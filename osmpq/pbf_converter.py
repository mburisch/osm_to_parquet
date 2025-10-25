from __future__ import annotations

from collections.abc import Generator
from collections.abc import Iterable

import fsspec
import pyarrow as pa
import pyarrow.dataset
import pyarrow.fs
import pyarrow.parquet as pq
from more_itertools import batched
from tqdm import tqdm

from osmpq.arrow import ARROW_BLOB_SCHEMA
from osmpq.arrow import ParquetBatchWriter
from osmpq.arrow import WriterConfig
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import read_blobs


def read_blob_data(filename: str) -> Iterable[BlobData]:
    with fsspec.open(filename, "rb") as fin:
        yield from read_blobs(fin)


def to_record_batches(blobs: Iterable[BlobData], batch_size: int) -> Iterable[pa.RecordBatch]:
    for batch in batched(blobs, batch_size):
        data = {
            "blob_type": [blob.header.type for blob in batch],
            "header_data": [blob.header_data for blob in batch],
            "blob_data": [blob.blob_data for blob in batch],
        }
        record = pa.RecordBatch.from_pydict(data, schema=ARROW_BLOB_SCHEMA)
        yield record


def write_records(path: str, records: Iterable[pa.RecordBatch], config: WriterConfig) -> None:
    fs, base_path = pa.fs.FileSystem.from_uri(path)
    fs.create_dir(base_path, recursive=True)
    fs.delete_dir_contents(base_path)

    writer = ParquetBatchWriter(
        fs=fs,
        base_path=base_path,
        filename_template="osm_pbf_blobs_part_{index:05d}.parquet",
        schema=ARROW_BLOB_SCHEMA,
        config=config,
    )

    with writer:
        for record in records:
            writer.write(record)


class ProcessorConfig:
    pbf_batch_size: int = 16

    max_rows_per_group: int = 16
    max_file_size: int = 128 * 1024 * 1024


def pbf_to_blobs(pbf_filename: str, output_path: str, config: ProcessorConfig) -> None:
    writer_config = WriterConfig(
        max_rows_per_group=config.max_rows_per_group,
        max_file_size=config.max_file_size,
    )

    blobs = read_blob_data(pbf_filename)
    blobs = tqdm(blobs, desc="Reading blobs", unit_scale=True)
    records = to_record_batches(blobs, config.pbf_batch_size)
    write_records(output_path, records, writer_config)
