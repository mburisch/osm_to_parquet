from __future__ import annotations

import zlib
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO

import zstd

from osmpq.protos.fileformat_pb2 import Blob
from osmpq.protos.fileformat_pb2 import BlobHeader
from osmpq.protos.osmformat_pb2 import HeaderBlock
from osmpq.protos.osmformat_pb2 import PrimitiveBlock


class BlobType(Enum):
    OSM_HEADER = "OSMHeader"
    OSM_DATA = "OSMData"


@dataclass(frozen=True)
class BlobData:
    header: BlobHeader
    header_data: bytes
    blob_data: bytes


def read_blob_data(source: BinaryIO) -> BlobData | None:
    data = source.read(4)
    if len(data) == 0:
        return None

    header_size = int.from_bytes(data, "big")

    header_data = source.read(header_size)
    blob_header = BlobHeader.FromString(header_data)

    blob_data = source.read(blob_header.datasize)

    return BlobData(header=blob_header, header_data=header_data, blob_data=blob_data)


def read_blobs(source: BinaryIO) -> Generator[BlobData, None, None]:
    while True:
        data = read_blob_data(source)
        if data is None:
            return
        yield data


def decompress_blob(blob: Blob) -> bytes:
    match blob.WhichOneof("data"):
        case "raw":
            return blob.raw
        case "zlib_data":
            return zlib.decompress(blob.zlib_data)
        case "zstd_data":
            return zstd.decompress(blob.zstd_data)
        case _:
            raise ValueError("Blob has no data")


def decode_blob(header: BlobHeader, blob_data: bytes) -> HeaderBlock | PrimitiveBlock:
    blob = Blob.FromString(blob_data)
    data = decompress_blob(blob)
    match header.type:
        case BlobType.OSM_HEADER.value:
            return HeaderBlock.FromString(data)
        case BlobType.OSM_DATA.value:
            return PrimitiveBlock.FromString(data)
        case _:
            raise ValueError(f"Unknown blob type: {header.type}")


def decode_header_blob(blob_data: bytes) -> HeaderBlock:
    blob = Blob.FromString(blob_data)
    data = decompress_blob(blob)
    return HeaderBlock.FromString(data)


def decode_primtive_blob(blob_data: bytes) -> PrimitiveBlock:
    blob = Blob.FromString(blob_data)
    data = decompress_blob(blob)
    return PrimitiveBlock.FromString(data)
