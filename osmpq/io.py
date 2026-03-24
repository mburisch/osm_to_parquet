from __future__ import annotations

import json
import os.path
import uuid
from dataclasses import dataclass
from typing import Any
from typing import Iterable

import fsspec
import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet as pq
from fsspec.spec import AbstractFileSystem

from osmpq.arrow import ARROW_BLOB_SCHEMA
from osmpq.arrow import ARROW_NODE_SCHEMA
from osmpq.arrow import ARROW_RELATION_SCHEMA
from osmpq.arrow import ARROW_WAY_SCHEMA
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import decode_header_blob_to_dict
from osmpq.osm.blob import read_blobs


def get_fs(path: str) -> tuple[AbstractFileSystem, str]:
    fs, base_path = fsspec.core.url_to_fs(path)
    return fs, base_path


def read_blobs_from_pbf(filename: str) -> Iterable[BlobData]:
    with fsspec.open(filename, "rb") as fin:
        yield from read_blobs(fin)


def write_header_as_json(filename: str, blob_data: bytes) -> None:
    header = decode_header_blob_to_dict(blob_data)

    with fsspec.open(filename, "wb") as fout:
        fout.write(json.dumps(header, indent=2).encode())


def clear_output_path(path: str) -> None:
    fs, base_path = get_fs(path)
    if fs.exists(base_path):
        fs.rm(base_path, recursive=True)
    fs.makedirs(base_path, exist_ok=True)


@dataclass
class WriterConfig:
    max_rows_per_row_group: int | None = None
    max_rows_per_file: int | None = None
    max_file_size_bytes: int | None = None


@dataclass
class Writer:
    writer: pq.ParquetWriter
    written_rows: int = 0
    written_batches: int = 0
    written_bytes: int = 0

    @classmethod
    def create(cls, filename: str, schema: pa.Schema) -> Writer:
        fs, path = get_fs(filename)
        writer = pq.ParquetWriter(
            path,
            schema=schema,
            flavor="spark",
            filesystem=fs,
            compression="zstd",
        )
        return cls(writer=writer)

    def write(self, batch: pa.RecordBatch) -> None:
        self.writer.write_batch(batch)
        self.written_rows += batch.num_rows
        self.written_batches += 1
        self.written_bytes += batch.nbytes

    def close(self) -> None:
        self.writer.close()


class MultiParquetWriter:
    def __init__(self, path: str, filename_template: str, schema: pa.Schema, config: WriterConfig) -> None:
        self.path = path
        self.filename_template = filename_template
        self.schema = schema
        self.writer_config = config
        self.unique_id = str(uuid.uuid4()).replace("-", "_")
        self._file_index = 0

        self._writer: Writer | None = None

    def _get_writer(self) -> Writer:
        if self._writer is None:
            self._file_index += 1
            filename = os.path.join(
                self.path, self.filename_template.format(file_id=self.unique_id, index=self._file_index)
            )
            self._writer = Writer.create(
                filename=filename,
                schema=self.schema,
            )

        return self._writer

    def write(self, batch: pa.RecordBatch | None) -> None:
        if batch is None or batch.num_rows == 0:
            return
        writer = self._get_writer()
        writer.write(batch)
        if self._should_switch_writer():
            self.close()

    def _should_switch_writer(self) -> bool:
        if self._writer is None:
            return False

        if self.writer_config.max_rows_per_file:
            if self._writer.written_rows >= self.writer_config.max_rows_per_file:
                return True

        if self.writer_config.max_file_size_bytes is not None:
            if self._writer.written_bytes >= self.writer_config.max_file_size_bytes:
                return True

        return False

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> MultiParquetWriter:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


@dataclass(frozen=True)
class ElementBatch:
    nodes: pa.RecordBatch | None = None
    ways: pa.RecordBatch | None = None
    relations: pa.RecordBatch | None = None


class ElementsWriter:
    def __init__(self, path: str, config: WriterConfig) -> None:
        self.nodes = MultiParquetWriter(
            path=os.path.join(path, "nodes/"),
            filename_template="nodes_{file_id}_{index:05d}.parquet",
            schema=ARROW_NODE_SCHEMA,
            config=config,
        )
        self.ways = MultiParquetWriter(
            path=os.path.join(path, "ways/"),
            filename_template="ways_{file_id}_{index:05d}.parquet",
            schema=ARROW_WAY_SCHEMA,
            config=config,
        )
        self.relations = MultiParquetWriter(
            path=os.path.join(path, "relations/"),
            filename_template="relations_{file_id}_{index:05d}.parquet",
            schema=ARROW_RELATION_SCHEMA,
            config=config,
        )

    def write(self, batch: ElementBatch) -> None:
        self.nodes.write(batch.nodes)
        self.ways.write(batch.ways)
        self.relations.write(batch.relations)

    def __enter__(self) -> ElementsWriter:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.nodes.close()
        self.ways.close()
        self.relations.close()


def read_blobs_from_parquet(filename: str) -> Iterable[pa.RecordBatch]:
    fs, path = get_fs(filename)
    ds = pa.dataset.dataset(path, filesystem=fs, format="parquet", schema=ARROW_BLOB_SCHEMA)
    yield from ds.to_batches()
