from __future__ import annotations

import os.path
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any
from typing import BinaryIO

import fsspec
import fsspec.core
from tqdm import tqdm

from osmpq.osm.blob import BlobData
from osmpq.protos.fileformat_pb2 import BlobHeader


def read_blob_data(fp: BinaryIO) -> BlobData | None:
    data = fp.read(4)
    if len(data) == 0:
        return None

    header_size = int.from_bytes(data, "big")

    header_data = fp.read(header_size)
    blob_header = BlobHeader.FromString(header_data)

    blob_data = fp.read(blob_header.datasize)

    return BlobData(header=blob_header, header_data=header_data, blob_data=blob_data)


def write_blob_data(fp: BinaryIO, blob_data: BlobData) -> int:
    header_size = len(blob_data.header_data)
    fp.write(header_size.to_bytes(4, "big"))
    fp.write(blob_data.header_data)
    fp.write(blob_data.blob_data)

    return 4 + header_size + len(blob_data.blob_data)


def read_blobs(fp: BinaryIO) -> Generator[BlobData, None, None]:
    while True:
        data = read_blob_data(fp)
        if data is None:
            return
        yield data


class Writer:
    def __init__(self, output_folder: str, max_file_size_bytes: int) -> None:
        self.output_folder = output_folder
        self.max_file_size_bytes = max_file_size_bytes
        self.header: BinaryIO | None = None
        self.blobs: BinaryIO | None = None
        self.file_index: int = 0
        self.current_file_size: int = 0

    def header_writer(self) -> BinaryIO:
        if self.header is None:
            self.header = fsspec.open(os.path.join(self.output_folder, "header.pbf"), "wb").open()
        return self.header

    def blob_writer(self) -> BinaryIO:
        if self.blobs is not None and self.current_file_size >= self.max_file_size_bytes:
            self.blobs.close()
            self.blobs = None
            self.current_file_size = 0

        if self.blobs is None:
            self.file_index += 1
            filename = os.path.join(self.output_folder, f"blobs_{self.file_index:05d}.pbf")
            self.blobs = fsspec.open(filename, "wb").open()

        return self.blobs

    def write(self, blob: BlobData) -> None:
        match blob.header.type:
            case "OSMHeader":
                write_blob_data(self.header_writer(), blob)
            case "OSMData":
                size = write_blob_data(self.blob_writer(), blob)
                self.current_file_size += size

    def __enter__(self) -> Writer:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if self.header is not None:
            self.header.close()
        if self.blobs is not None:
            self.blobs.close()


def split_pbf(pbf_filename: str, output_folder: str, max_file_size_bytes: int) -> None:
    with Writer(output_folder, max_file_size_bytes) as writer, fsspec.open(pbf_filename, "rb") as fin:
        blobs = read_blobs(fin)
        for blob in tqdm(blobs, desc="Writing blobs", unit_scale=True):
            writer.write(blob)


def clear_output_path(path: str) -> None:
    fs, base_path = fsspec.core.url_to_fs(path)
    if fs.exists(base_path):
        fs.rm(base_path, recursive=True)
    fs.makedirs(base_path, exist_ok=True)
