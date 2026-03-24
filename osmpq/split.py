from __future__ import annotations

import os.path
from typing import Any
from typing import BinaryIO

import fsspec
from tqdm import tqdm

from osmpq.osm.blob import BlobData
from osmpq.osm.blob import read_blobs


def write_blob_data(fp: BinaryIO, blob_data: BlobData) -> int:
    header_size = len(blob_data.header_data)
    fp.write(header_size.to_bytes(4, "big"))
    fp.write(blob_data.header_data)
    fp.write(blob_data.blob_data)

    return 4 + header_size + len(blob_data.blob_data)


class Writer:
    def __init__(self, output_folder: str, max_file_size_bytes: int) -> None:
        self.output_folder = output_folder
        self.max_file_size_bytes = max_file_size_bytes
        self.header: BlobData | None = None
        self.blobs: BinaryIO | None = None
        self.file_index: int = 0
        self.current_file_size: int = 0

    def blob_writer(self) -> BinaryIO:
        if self.blobs is not None and self.current_file_size >= self.max_file_size_bytes:
            self.blobs.close()
            self.blobs = None
            self.current_file_size = 0

        if self.blobs is None:
            self.file_index += 1
            filename = os.path.join(self.output_folder, f"blobs_{self.file_index:05d}.pbf")
            self.blobs = fsspec.open(filename, "wb").open()
            if self.header is not None:
                write_blob_data(self.blobs, self.header)

        return self.blobs

    def write(self, blob: BlobData) -> int:
        if blob.header.type == "OSMHeader":
            self.header = blob

        size = write_blob_data(self.blob_writer(), blob)
        self.current_file_size += size
        return size

    def __enter__(self) -> Writer:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if self.blobs is not None:
            self.blobs.close()


def split_pbf(pbf_filename: str, output_folder: str, max_file_size_bytes: int) -> None:
    with (
        Writer(output_folder, max_file_size_bytes) as writer,
        fsspec.open(pbf_filename, "rb", cache_type="readahead") as fin,
    ):
        blobs = read_blobs(fin)
        with tqdm(desc="Writing blobs", unit="B", unit_scale=True) as pbar:
            for blob in blobs:
                size = writer.write(blob)
                pbar.update(size)
