from __future__ import annotations

import json
from typing import Any
from typing import Iterable
from typing import Protocol

import pyarrow as pa
from google.protobuf.json_format import MessageToDict
from pyspark.sql import DataFrame

from osmpq.arrow import ARROW_NODE_SCHEMA
from osmpq.arrow import ARROW_RELATION_SCHEMA
from osmpq.arrow import ARROW_WAY_SCHEMA
from osmpq.arrow import ParquetBatchWriter
from osmpq.arrow import WriterConfig
from osmpq.arrow import get_node_batch
from osmpq.arrow import get_relation_batch
from osmpq.arrow import get_way_batch
from osmpq.arrow import record_batch_for_nodes
from osmpq.helper import join_path
from osmpq.osm.blob import BlobType
from osmpq.osm.blob import decode_header_blob
from osmpq.osm.blob import decode_primtive_blob
from osmpq.osm.elements import PrimitiveBlockDecoder
from osmpq.osm.elements import decode_nodes
from osmpq.osm.elements import decode_relations
from osmpq.osm.elements import decode_ways


class Writer:
    def __init__(self, root_path: str, config: WriterConfig) -> None:
        fs, base_path = pa.fs.FileSystem.from_uri(root_path)

        self.fs = fs
        self.base_path = base_path

        self.nodes_writer = ParquetBatchWriter(
            fs=fs,
            base_path=join_path(base_path, "nodes/"),
            filename_template="nodes_{file_id}_{index:05d}.parquet",
            schema=ARROW_NODE_SCHEMA,
            config=config,
        )
        self.ways_writer = ParquetBatchWriter(
            fs=fs,
            base_path=join_path(base_path, "ways/"),
            filename_template="ways_{file_id}_{index:05d}.parquet",
            schema=ARROW_WAY_SCHEMA,
            config=config,
        )
        self.relations_writer = ParquetBatchWriter(
            fs=fs,
            base_path=join_path(base_path, "relations/"),
            filename_template="relations_{file_id}_{index:05d}.parquet",
            schema=ARROW_RELATION_SCHEMA,
            config=config,
        )

    def write_header(self, blob_data: bytes) -> None:
        header = decode_header_blob(blob_data)
        message = MessageToDict(header, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)

        with self.fs.open_output_stream(join_path(self.base_path, "header.json"), compression=None) as stream:
            stream.write(json.dumps(message).encode())

    def write_elements(self, blob_data: bytes) -> None:
        block = decode_primtive_blob(bytes(blob_data))
        decoder = PrimitiveBlockDecoder(block)

        # self.nodes_writer.write(get_node_batch(decode_nodes(decoder)))
        self.nodes_writer.write(record_batch_for_nodes(list(decode_nodes(decoder))))

        # self.ways_writer.write(get_way_batch(decode_ways(decoder)))
        # self.relations_writer.write(get_relation_batch(decode_relations(decoder)))

    def __enter__(self) -> Writer:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.nodes_writer.close()
        self.ways_writer.close()
        self.relations_writer.close()


class BlobRow(Protocol):
    blob_type: str
    blob_data: bytes


def make_processor(root_path: str, config: WriterConfig) -> Any:
    def process(rows: Iterable[BlobRow]) -> None:
        with Writer(root_path, config=config) as writer:
            for row in rows:
                match row.blob_type:
                    case BlobType.OSM_HEADER.value:
                        writer.write_header(row.blob_data)
                    case BlobType.OSM_DATA.value:
                        writer.write_elements(row.blob_data)

    return process


def prepare_output_path(output_path: str) -> None:
    fs, path = pa.fs.FileSystem.from_uri(output_path)
    fs.create_dir(path, recursive=True)
    fs.delete_dir_contents(path)
    fs.create_dir(join_path(path, "nodes/"))
    fs.create_dir(join_path(path, "ways/"))
    fs.create_dir(join_path(path, "relations/"))


def blobs_to_elements(blobs: DataFrame, output_path: str) -> None:
    prepare_output_path(output_path)
    config = WriterConfig(max_file_size=128 * 1024 * 1024)
    processor = make_processor(output_path, config=config)

    blobs.foreachPartition(lambda partition: processor(partition))
