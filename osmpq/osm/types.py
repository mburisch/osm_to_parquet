from __future__ import annotations

from dataclasses import dataclass

OsmTags = dict[str, str]


@dataclass(frozen=True)
class OsmInfo:
    version: int | None
    timestamp: int | None
    changeset: int | None
    uid: int | None
    user_sid: str | None

    @classmethod
    def default(cls) -> OsmInfo:
        return cls(None, None, None, None, None)


@dataclass(frozen=True)
class OsmNode:
    id: int
    info: OsmInfo
    tags: OsmTags | None
    latitude: float
    longitude: float


@dataclass(frozen=True)
class OsmWay:
    id: int
    info: OsmInfo
    tags: OsmTags | None
    nodes: list[int]


@dataclass(frozen=True)
class OsmRelationMember:
    id: int
    role: str
    type: str


@dataclass(frozen=True)
class OsmRelation:
    id: int
    info: OsmInfo
    tags: OsmTags | None
    members: list[OsmRelationMember]
