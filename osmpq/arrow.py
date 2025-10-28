from __future__ import annotations

import uuid
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


def record_batch_for_nodes(nodes: list[OsmNode]) -> pa.RecordBatch | None:
    if not nodes:
        return None

    ids = [node.id for node in nodes]
    versions = [node.info.version for node in nodes]
    latitudes = [node.latitude for node in nodes]
    longitudes = [node.longitude for node in nodes]
    timestamps = [node.info.timestamp for node in nodes]
    changesets = [node.info.changeset for node in nodes]
    uids = [node.info.uid for node in nodes]
    user_sids = [node.info.user_sid for node in nodes]

    tags = []
    for node in nodes:
        if node.tags is not None:
            tags.append([(k, v) for k, v in node.tags.items()])
        else:
            tags.append(None)

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=pa.map_(pa.string(), pa.string())),
        pa.array(latitudes, type=pa.float64()),
        pa.array(longitudes, type=pa.float64()),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]

    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_NODE_SCHEMA)


def record_batch_for_ways(ways: list[OsmWay]) -> pa.RecordBatch | None:
    if not ways:
        return None

    ids = [way.id for way in ways]
    versions = [way.info.version for way in ways]
    nodes = [way.nodes for way in ways]
    timestamps = [way.info.timestamp for way in ways]
    changesets = [way.info.changeset for way in ways]
    uids = [way.info.uid for way in ways]
    user_sids = [way.info.user_sid for way in ways]

    tags = []
    for way in ways:
        if way.tags is not None:
            tags.append([(k, v) for k, v in way.tags.items()])
        else:
            tags.append(None)

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=pa.map_(pa.string(), pa.string())),
        pa.array(nodes, type=pa.list_(pa.int64())),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]

    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_WAY_SCHEMA)


def record_batch_for_relations(relations: list[OsmRelation]) -> pa.RecordBatch | None:
    if not relations:
        return None

    ids = [relation.id for relation in relations]
    versions = [relation.info.version for relation in relations]
    timestamps = [relation.info.timestamp for relation in relations]
    changesets = [relation.info.changeset for relation in relations]
    uids = [relation.info.uid for relation in relations]
    user_sids = [relation.info.user_sid for relation in relations]

    tags = []
    for relation in relations:
        if relation.tags is not None:
            tags.append([(k, v) for k, v in relation.tags.items()])
        else:
            tags.append(None)

    members = []
    for relation in relations:
        if relation.members is not None:
            members.append([(m.id, m.role, m.type) for m in relation.members])
        else:
            members.append(None)

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=pa.map_(pa.string(), pa.string())),
        pa.array(members),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]

    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_RELATION_SCHEMA)


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

    def write(self, batch: pa.RecordBatch | None) -> None:
        if batch is None:
            return
        writer = self._get_writer()
        writer.write(batch)
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
