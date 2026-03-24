from __future__ import annotations

import zlib
from collections.abc import Generator
from compression import zstd
from dataclasses import dataclass
from enum import StrEnum
from typing import BinaryIO

from google.protobuf.json_format import MessageToDict

from osmpq.protos.fileformat_pb2 import Blob
from osmpq.protos.fileformat_pb2 import BlobHeader
from osmpq.protos.osmformat_pb2 import HeaderBlock
from osmpq.protos.osmformat_pb2 import PrimitiveBlock


class BlobType(StrEnum):
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
        case BlobType.OSM_HEADER:
            return HeaderBlock.FromString(data)
        case BlobType.OSM_DATA:
            return PrimitiveBlock.FromString(data)
        case _:
            raise ValueError(f"Unknown blob type: {header.type}")


def decode_blob_data(blob_data: bytes) -> bytes:
    blob = Blob.FromString(blob_data)
    data = decompress_blob(blob)
    return data


def decode_header_blob(blob_data: bytes) -> HeaderBlock:
    data = decode_blob_data(blob_data)
    return HeaderBlock.FromString(data)


def decode_primtive_blob(blob_data: bytes) -> PrimitiveBlock:
    data = decode_blob_data(blob_data)
    return PrimitiveBlock.FromString(data)


def decode_header_blob_to_dict(blob_data: bytes) -> dict:
    header = decode_header_blob(blob_data)
    message = MessageToDict(header, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)
    message["bbox"] = {k: int(v) / 10**9 for k, v in message["bbox"].items()}
    message["osmosis_replication_timestamp"] = int(message["osmosis_replication_timestamp"])
    message["osmosis_replication_sequence_number"] = int(message["osmosis_replication_sequence_number"])
    return message
