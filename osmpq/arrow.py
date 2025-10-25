from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from osmpq.helper import join_path
from osmpq.osm.types import OsmNode
from osmpq.osm.types import OsmRelation
from osmpq.osm.types import OsmWay

ARROW_BLOB_SCHEMA = pa.schema(
    [
        ("blob_type", pa.string()),
        ("header_data", pa.binary()),
        ("blob_data", pa.binary()),
    ]
)

ARROW_NODE_FIELDS = [
    pa.field("id", pa.int64()),
    pa.field("version", pa.int32()),
    pa.field("tags", pa.map_(pa.string(), pa.string())),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("timestamp", pa.int64()),
    pa.field("changeset", pa.int64()),
    pa.field("uid", pa.int64()),
    pa.field("user_sid", pa.string()),
]

ARROW_NODE_SCHEMA = pa.schema(ARROW_NODE_FIELDS)


ARROW_WAY_FIELD = [
    pa.field("id", pa.int64()),
    pa.field("version", pa.int32()),
    pa.field("tags", pa.map_(pa.string(), pa.string())),
    pa.field("nodes", pa.list_(pa.int64())),
    pa.field("timestamp", pa.int64()),
    pa.field("changeset", pa.int64()),
    pa.field("uid", pa.int64()),
    pa.field("user_sid", pa.string()),
]


ARROW_WAY_SCHEMA = pa.schema(ARROW_WAY_FIELD)


ARROW_RELATION_FIELDS = [
    pa.field("id", pa.int64()),
    pa.field("version", pa.int32()),
    pa.field("tags", pa.map_(pa.string(), pa.string())),
    pa.field(
        "members",
        pa.list_(
            pa.struct(
                [
                    pa.field("id", pa.int64()),
                    pa.field("role", pa.string()),
                    pa.field("type", pa.string()),
                ]
            )
        ),
    ),
    pa.field("timestamp", pa.int64()),
    pa.field("changeset", pa.int64()),
    pa.field("uid", pa.int64()),
    pa.field("user_sid", pa.string()),
]

ARROW_RELATION_SCHEMA = pa.schema(ARROW_RELATION_FIELDS)


ARROW_ELEMENT_SCHEMA = pa.schema(
    [
        pa.field("nodes", pa.list_(pa.struct(ARROW_NODE_FIELDS))),
        pa.field("ways", pa.list_(pa.struct(ARROW_WAY_FIELD))),
        pa.field("relations", pa.list_(pa.struct(ARROW_RELATION_FIELDS))),
    ]
)


def record_batch_for_nodes(nodes: list[OsmNode]) -> pa.RecordBatch:
    n = len(nodes)
    ids = [0] * n
    versions = [0] * n
    tags = [None] * n
    latitude = [0.0] * n
    longitude = [0.0] * n
    timestamp = [0] * n
    changeset = [0] * n
    uid = [0] * n
    user_sid = [0] * n
    for i, node in enumerate(nodes):
        ids[i] = node.id
        versions[i] = node.info.version
        if node.tags is not None:
            tags[i] = [(k, v) for k, v in node.tags.items()]
        latitude[i] = node.latitude
        longitude[i] = node.longitude
        timestamp[i] = node.info.timestamp
        changeset[i] = node.info.changeset
        uid[i] = node.info.uid
        user_sid[i] = node.info.user_sid

    return pa.RecordBatch.from_pydict(
        {
            "id": ids,
            "version": versions,
            "tags": tags,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp,
            "changeset": changeset,
            "uid": uid,
            "user_sid": user_sid,
        },
        schema=ARROW_NODE_SCHEMA,
    )


# return {
#     "id": node.id,
#     "version": node.info.version,
#     "tags": node.tags,
#     "latitude": node.latitude,
#     "longitude": node.longitude,
#     "timestamp": node.info.timestamp,
#     "changeset": node.info.changeset,
#     "uid": node.info.uid,
#     "user_sid": node.info.user_sid,
# }


def get_arrow_node(node: OsmNode) -> dict[str, Any]:
    return {
        "id": node.id,
        "version": node.info.version,
        "tags": node.tags,
        "latitude": node.latitude,
        "longitude": node.longitude,
        "timestamp": node.info.timestamp,
        "changeset": node.info.changeset,
        "uid": node.info.uid,
        "user_sid": node.info.user_sid,
    }


def get_arrow_way(way: OsmWay) -> dict[str, Any]:
    return {
        "id": way.id,
        "version": way.info.version,
        "tags": way.tags,
        "nodes": way.nodes,
        "timestamp": way.info.timestamp,
        "changeset": way.info.changeset,
        "uid": way.info.uid,
        "user_sid": way.info.user_sid,
    }


def get_arrow_relation(relation: OsmRelation) -> dict[str, Any]:
    return {
        "id": relation.id,
        "version": relation.info.version,
        "tags": relation.tags,
        "members": [{"id": member.id, "role": member.role, "type": member.type} for member in relation.members],
        "timestamp": relation.info.timestamp,
        "changeset": relation.info.changeset,
        "uid": relation.info.uid,
        "user_sid": relation.info.user_sid,
    }


def get_node_batch(nodes: Iterable[OsmNode]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([get_arrow_node(n) for n in nodes], schema=ARROW_NODE_SCHEMA)


def get_way_batch(ways: Iterable[OsmWay]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([get_arrow_way(w) for w in ways], schema=ARROW_WAY_SCHEMA)


def get_relation_batch(relations: Iterable[OsmRelation]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([get_arrow_relation(r) for r in relations], schema=ARROW_RELATION_SCHEMA)


@dataclass
class WriterConfig:
    max_rows_per_group: int | None = None
    max_rows_per_file: int | None = None
    max_file_size: int | None = None


@dataclass
class Writer:
    filename: str
    writer: pq.ParquetWriter
    written_rows: int = 0
    written_batches: int = 0

    def write(self, batch: pa.RecordBatch) -> None:
        self.writer.write_batch(batch)
        self.written_rows += batch.num_rows
        self.written_batches += 1

    def close(self) -> None:
        self.writer.close()


class ParquetBatchWriter:
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

        self.fs.create_dir(base_path, recursive=True)
        self._writer: Writer | None = None

    def _get_writer(self) -> Writer:
        if self._writer is None:
            self._file_index += 1
            filename = join_path(
                self.base_path, self.filename_template.format(file_id=self.unique_id, index=self._file_index)
            )
            writer = pq.ParquetWriter(
                filename,
                schema=self.schema,
                flavor="spark",
                filesystem=self.fs,
            )
            self._writer = Writer(
                filename=filename,
                writer=writer,
            )

        return self._writer

    def write(self, nodes: pa.RecordBatch) -> int:
        writer = self._get_writer()
        writer.write(nodes)
        if self._should_switch_writer():
            self.close()

    def _should_switch_writer(self) -> None:
        if self._writer is None:
            return False

        if self.writer_config.max_rows_per_file:
            if self._writer.written_rows >= self.writer_config.max_rows_per_file:
                return True

        if self.writer_config.max_file_size is not None:
            if self._writer.written_batches % 10 == 0:
                if self.fs.get_file_info(self._writer.filename).size >= self.writer_config.max_file_size:
                    return True

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> ParquetBatchWriter:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
