from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
from more_itertools import batched
from tqdm import tqdm

from osmpq.arrow import ARROW_BLOB_SCHEMA
from osmpq.arrow import record_batches_for_blobs
from osmpq.io import MultiParquetWriter
from osmpq.io import WriterConfig
from osmpq.io import clear_output_path
from osmpq.io import read_blobs_from_pbf
from osmpq.io import write_header_as_json
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import BlobType
from osmpq.protos.fileformat_pb2 import BlobHeader


def to_record_batches(blobs: Iterable[BlobData], batch_size: int) -> Iterable[pa.RecordBatch]:
    for batch in batched(blobs, batch_size):
        yield record_batches_for_blobs(batch)


def from_record_batch(batch: pa.RecordBatch) -> Iterable[BlobData]:
    data = batch.to_pydict()
    for header_data, blob_data in zip(data["header_data"], data["blob_data"]):
        yield BlobData(header=BlobHeader.FromString(header_data), header_data=header_data, blob_data=blob_data)


def write_record_batches(records: Iterable[pa.RecordBatch], writer: MultiParquetWriter) -> None:
    with writer:
        for record in records:
            writer.write(record)


def create_blobs_writer(path: str, config: WriterConfig) -> MultiParquetWriter:
    return MultiParquetWriter(
        path=path,
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
    clear_output_path(output_path)
    writer = create_blobs_writer(output_path, writer_config)

    blobs = read_blobs_from_pbf(pbf_filename)
    if header_output_filename is not None:
        blobs = header_extractor(blobs, header_output_filename)

    blobs = tqdm(blobs, desc="Processing blobs", unit_scale=True)
    records = to_record_batches(blobs, writer_config.max_rows_per_row_group or 16)
    write_record_batches(records, writer)
