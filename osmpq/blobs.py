from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
from more_itertools import batched
from tqdm import tqdm

from osmpq.arrow import ARROW_BLOB_SCHEMA
from osmpq.arrow import record_batches_for_blobs
from osmpq.io import ParquetBatchWriter
from osmpq.io import WriterConfig
from osmpq.io import read_blobs_from_pbf
from osmpq.io import write_header_as_json
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import BlobType


def to_record_batches(blobs: Iterable[BlobData], batch_size: int) -> Iterable[pa.RecordBatch]:
    for batch in batched(blobs, batch_size):
        yield record_batches_for_blobs(batch)


def write_record_batches(records: Iterable[pa.RecordBatch], writer: ParquetBatchWriter) -> None:
    with writer:
        for record in records:
            writer.write(record)


def create_writer(path: str, config: WriterConfig) -> ParquetBatchWriter:
    fs, base_path = pa.fs.FileSystem.from_uri(path)
    return ParquetBatchWriter(
        fs=fs,
        base_path=base_path,
        filename_template="osm_pbf_blobs_part_{index:05d}.parquet",
        schema=ARROW_BLOB_SCHEMA,
        config=config,
    )


def header_extractor(blobs: Iterable[BlobData], filename: str) -> Iterable[BlobData]:
    for blob in blobs:
        if blob.header.type == BlobType.OSM_HEADER.value:
            write_header_as_json(filename, blob.blob_data)

        yield blob


def pbf_to_blob_parquet(
    pbf_filename: str,
    output_path: str,
    header_output_filename: str | None,
    writer_config: WriterConfig,
) -> None:
    writer = create_writer(output_path, writer_config)

    blobs = read_blobs_from_pbf(pbf_filename)
    if header_output_filename is not None:
        blobs = header_extractor(blobs, header_output_filename)

    blobs = tqdm(blobs, desc="Reading blobs", unit_scale=True)
    records = to_record_batches(blobs, writer_config.max_rows_per_row_group)
    write_record_batches(records, writer)
