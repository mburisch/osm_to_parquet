from __future__ import annotations

import json
import os.path
import uuid
from dataclasses import dataclass
from typing import Any
from typing import Iterable

import fsspec
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
from google.protobuf.json_format import MessageToDict

from osmpq.arrow import ARROW_BLOB_SCHEMA
from osmpq.arrow import ARROW_NODE_SCHEMA
from osmpq.arrow import ARROW_RELATION_SCHEMA
from osmpq.arrow import ARROW_WAY_SCHEMA
from osmpq.osm.blob import BlobData
from osmpq.osm.blob import decode_header_blob
from osmpq.osm.blob import read_blobs


def get_arrow_fs(path: str) -> tuple[pa.fs.FileSystem, str]:
    fs, base_path = pyarrow.fs.FileSystem.from_uri(path)
    return fs, base_path


def read_blobs_from_pbf(filename: str) -> Iterable[BlobData]:
    with fsspec.open(filename, "rb") as fin:
        yield from read_blobs(fin)


def write_header_as_json(filename: str, blob_data: bytes) -> None:
    header = decode_header_blob(blob_data)
    message = MessageToDict(header, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)

    with fsspec.open(filename, "wb") as fout:
        fout.write(json.dumps(message).encode())


def clear_output_path(fs: pa.fs.FileSytem, path: str) -> None:
    fs.create_dir(path, recursive=True)
    fs.delete_dir_contents(path)


@dataclass
class WriterConfig:
    max_rows_per_row_group: int | None = None
    max_row_group_size_bytes: int | None = None
    max_rows_per_file: int | None = None
    max_file_size_bytes: int | None = None


@dataclass
class Writer:
    writer: pq.ParquetWriter
    written_rows: int = 0
    written_batches: int = 0
    written_bytes: int = 0

    @classmethod
    def create(cls, fs: pa.fs.FileSytem, filename: str, schema: pa.Schema) -> Writer:
        writer = pq.ParquetWriter(
            filename,
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
    def __init__(
        self, fs: pa.fs.FileSytem, base_path: str, filename_template: str, schema: pa.Schema, config: WriterConfig
    ) -> None:
        self.fs = fs
        self.base_path = base_path
        self.filename_template = filename_template
        self.schema = schema
        self.writer_config = config
        self.unique_id = str(uuid.uuid4()).replace("-", "_")
        self._file_index = 0

        self._writer: Writer | None = None

        clear_output_path(fs, base_path)

    def _get_writer(self) -> Writer:
        if self._writer is None:
            self._file_index += 1
            filename = os.path.join(
                self.base_path, self.filename_template.format(file_id=self.unique_id, index=self._file_index)
            )
            self._writer = Writer.create(
                fs=self.fs,
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

        # if self._writer.written_batches % 10 == 0:
        #     if self.fs.get_file_info(self._writer.filename).size >= self.writer_config.max_file_size:
        #         return True

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
    def __init__(self, fs: pa.fs.FileSytem, base_path: str, config: WriterConfig) -> None:
        self.fs = fs

        self.nodes = MultiParquetWriter(
            fs=fs,
            base_path=os.path.join(base_path, "nodes/"),
            filename_template="nodes_{file_id}_{index:05d}.parquet",
            schema=ARROW_NODE_SCHEMA,
            config=config,
        )
        self.ways = MultiParquetWriter(
            fs=fs,
            base_path=os.path.join(base_path, "ways/"),
            filename_template="ways_{file_id}_{index:05d}.parquet",
            schema=ARROW_WAY_SCHEMA,
            config=config,
        )
        self.relations = MultiParquetWriter(
            fs=fs,
            base_path=os.path.join(base_path, "relations/"),
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


def create_elements_writer(path: str, config: WriterConfig) -> ElementsWriter:
    fs, base_path = get_arrow_fs(path)
    return ElementsWriter(
        fs=fs,
        base_path=base_path,
        config=config,
    )
