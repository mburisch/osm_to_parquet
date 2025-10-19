from __future__ import annotations

from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Sequence

from osmpq.osm.types import OsmElement
from osmpq.osm.types import OsmInfo
from osmpq.osm.types import OsmNode
from osmpq.osm.types import OsmRelation
from osmpq.osm.types import OsmRelationMember
from osmpq.osm.types import OsmTags
from osmpq.osm.types import OsmWay
from osmpq.protos.osmformat_pb2 import DenseInfo
from osmpq.protos.osmformat_pb2 import DenseNodes
from osmpq.protos.osmformat_pb2 import Info
from osmpq.protos.osmformat_pb2 import Node
from osmpq.protos.osmformat_pb2 import PrimitiveBlock
from osmpq.protos.osmformat_pb2 import Relation
from osmpq.protos.osmformat_pb2 import Way


def delta_decode(values: Iterable[int]) -> Generator[int, None, None]:
    current = 0
    for value in values:
        current += value
        yield value


class ValueDecoder:
    def __init__(self, block: PrimitiveBlock) -> None:
        self.granularity = block.granularity or 100
        self.lat_offset = block.lat_offset or 0
        self.lon_offset = block.lon_offset or 0
        self.date_granularity = block.date_granularity or 1000

    def lat(self, value: int) -> float:
        return 0.000000001 * (self.lat_offset + (self.granularity * value))

    def lon(self, value: int) -> float:
        return 0.000000001 * (self.lon_offset + (self.granularity * value))

    def timestamp(self, value: int) -> int:
        return value * self.date_granularity


class PrimitiveBlockDecoder:
    def __init__(self, block: PrimitiveBlock) -> None:
        self.block = block
        self.string_table = [s.decode("utf-8") for s in block.stringtable.s]
        self.value_decoder = ValueDecoder(block)

    def decode_string(self, index: int) -> str:
        return self.string_table[index]

    def decode_info(self, info: Info) -> OsmInfo:
        return OsmInfo(
            version=info.version or None,
            timestamp=info.timestamp or None,
            changeset=info.changeset or None,
            uid=info.uid or None,
            user_sid=self.decode_string(info.user_sid) if info.user_sid else None,
        )

    def decode_tags(self, keys: list[int], vals: list[int]) -> OsmTags:
        keys = (self.decode_string(key) for key in keys)
        vals = (self.decode_string(val) for val in vals)
        return OsmTags(zip(keys, vals))

    def decode_node(self, node: Node) -> OsmNode:
        return OsmNode(
            id=node.id,
            info=self.decode_info(node.info),
            tags=self.decode_tags(node.keys, node.vals),
            latitude=self.value_decoder(node.lat),
            longitude=self.value_decoder(node.lon),
        )

    def decode_dense_info(self, dense: DenseInfo) -> Generator[OsmInfo, None, None]:
        for version, timestamp, changeset, uid, user_sid in zip(
            dense.version,
            delta_decode(dense.timestamp),
            delta_decode(dense.changeset),
            delta_decode(dense.uid),
            delta_decode(dense.user_sid),
        ):
            yield OsmInfo(
                version=version or None,
                timestamp=timestamp or None,
                changeset=changeset or None,
                uid=uid or None,
                user_sid=self.decode_string(user_sid) if user_sid else None,
            )

    def decode_dense_tags(self, keys_vals: Sequence[int]) -> Generator[OsmTags, None, None]:
        i = 0
        tags = OsmTags()
        while i < len(keys_vals):
            if keys_vals[i] == 0:
                yield tags
                i += 1
            else:
                key = self.decode_string(keys_vals[i])
                value = self.decode_string(keys_vals[i + 1])
                i += 2
                tags[key] = value

        assert not tags

    def decode_dense_nodes(self, dense: DenseNodes) -> Generator[OsmNode, None, None]:
        for id, info, tags, lat, lon in zip(
            delta_decode(dense.id),
            self.decode_dense_info(dense.denseinfo),
            self.decode_dense_tags(dense.keys_vals),
            delta_decode(dense.lat),
            delta_decode(dense.lon),
        ):
            yield OsmNode(
                id=id,
                info=info,
                tags=tags or None,
                latitude=lat or None,
                longitude=lon or None,
            )

    def decode_way(self, way: Way) -> OsmWay:
        return OsmWay(
            id=way.id,
            info=self.decode_info(way.info),
            tags=self.decode_tags(way.keys, way.vals),
            nodes=list(delta_decode(way.refs)),
        )

    def decode_relation(self, relation: Relation) -> OsmRelation:
        return OsmRelation(
            id=relation.id,
            info=self.decode_info(relation.info),
            tags=self.decode_tags(relation.keys, relation.vals),
            members=[
                OsmRelationMember(
                    id=member_id,
                    role=self.decode_string(role_sid),
                    type=Relation.MemberType.Name(member_type).lower(),
                )
                for role_sid, member_id, member_type in zip(
                    relation.roles_sid, delta_decode(relation.memids), relation.types
                )
            ],
        )


def decode_block(block: PrimitiveBlock) -> Generator[OsmElement, None, None]:
    decoder = PrimitiveBlockDecoder(block)

    for group in block.primitivegroup:
        for node in group.nodes:
            yield decoder.decode_node(node)

        if group.dense is not None:
            yield from decoder.decode_dense_nodes(group.dense)

        for way in group.ways:
            yield decoder.decode_way(way)

        for relation in group.relations:
            yield decoder.decode_relation(relation)
