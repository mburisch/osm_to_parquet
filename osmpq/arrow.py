from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from osmpq.osm.types import OsmNode
from osmpq.osm.types import OsmRelation
from osmpq.osm.types import OsmTags
from osmpq.osm.types import OsmWay

ARROW_TAG_FIELD = pa.map_(pa.string(), pa.string())

ARROW_NODE_FIELDS = [
    pa.field("id", pa.int64()),
    pa.field("version", pa.int32()),
    pa.field("tags", ARROW_TAG_FIELD),
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
    pa.field("tags", ARROW_TAG_FIELD),
    pa.field("nodes", pa.list_(pa.int64())),
    pa.field("timestamp", pa.int64()),
    pa.field("changeset", pa.int64()),
    pa.field("uid", pa.int64()),
    pa.field("user_sid", pa.string()),
]


ARROW_WAY_SCHEMA = pa.schema(ARROW_WAY_FIELD)


ARROW_RELATION_MEMBERS_FIELD = pa.list_(
    pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("role", pa.string()),
            pa.field("type", pa.string()),
        ]
    )
)

ARROW_RELATION_FIELDS = [
    pa.field("id", pa.int64()),
    pa.field("version", pa.int32()),
    pa.field("tags", ARROW_TAG_FIELD),
    pa.field("members", ARROW_RELATION_MEMBERS_FIELD),
    pa.field("timestamp", pa.int64()),
    pa.field("changeset", pa.int64()),
    pa.field("uid", pa.int64()),
    pa.field("user_sid", pa.string()),
]

ARROW_RELATION_SCHEMA = pa.schema(ARROW_RELATION_FIELDS)


def _get_tags_array(tags: OsmTags) -> Sequence[tuple[str, str]]:
    return list(tags.items())


def record_batch_for_nodes(nodes: Sequence[OsmNode]) -> pa.RecordBatch | None:
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

    tags: list[Sequence[tuple[str, str]] | None] = [None] * len(nodes)
    for i, node in enumerate(nodes):
        if node.tags is not None:
            tags[i] = _get_tags_array(node.tags)

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=ARROW_TAG_FIELD),
        pa.array(latitudes, type=pa.float64()),
        pa.array(longitudes, type=pa.float64()),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_NODE_SCHEMA)


def record_batch_for_ways(ways: Sequence[OsmWay]) -> pa.RecordBatch | None:
    if not ways:
        return None

    ids = [way.id for way in ways]
    versions = [way.info.version for way in ways]
    nodes = [way.nodes for way in ways]
    timestamps = [way.info.timestamp for way in ways]
    changesets = [way.info.changeset for way in ways]
    uids = [way.info.uid for way in ways]
    user_sids = [way.info.user_sid for way in ways]

    tags: list[Sequence[tuple[str, str]] | None] = [None] * len(ways)
    for i, way in enumerate(ways):
        if way.tags is not None:
            tags[i] = _get_tags_array(way.tags)

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=ARROW_TAG_FIELD),
        pa.array(nodes, type=pa.list_(pa.int64())),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]

    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_WAY_SCHEMA)


def record_batch_for_relations(relations: Sequence[OsmRelation]) -> pa.RecordBatch | None:
    if not relations:
        return None

    ids = [relation.id for relation in relations]
    versions = [relation.info.version for relation in relations]
    timestamps = [relation.info.timestamp for relation in relations]
    changesets = [relation.info.changeset for relation in relations]
    uids = [relation.info.uid for relation in relations]
    user_sids = [relation.info.user_sid for relation in relations]

    tags: list[Sequence[tuple[str, str]] | None] = [None] * len(relations)
    for i, relation in enumerate(relations):
        if relation.tags is not None:
            tags[i] = _get_tags_array(relation.tags)

    members: list[list[tuple[int, str, str]] | None] = [None] * len(relations)
    for i, relation in enumerate(relations):
        if relation.members is not None:
            members[i] = [(m.id, m.role, m.type) for m in relation.members]

    arrays = [
        pa.array(ids, type=pa.int64()),
        pa.array(versions, type=pa.int32()),
        pa.array(tags, type=ARROW_TAG_FIELD),
        pa.array(members, type=ARROW_RELATION_MEMBERS_FIELD),
        pa.array(timestamps, type=pa.int64()),
        pa.array(changesets, type=pa.int64()),
        pa.array(uids, type=pa.int64()),
        pa.array(user_sids, type=pa.string()),
    ]

    return pa.RecordBatch.from_arrays(arrays, schema=ARROW_RELATION_SCHEMA)
