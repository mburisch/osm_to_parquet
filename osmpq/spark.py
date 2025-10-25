from __future__ import annotations

import itertools
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Generator

import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource
from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.datasource import InputPartition
from pyspark.sql.datasource import SimpleDataSourceStreamReader
from pyspark.sql.types import BinaryType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from osmpq.osm.blob import BlobData
from osmpq.osm.blob import read_blob_data
from osmpq.osm.blob import read_blobs


@contextmanager
def local_spark(
    config: dict[str, str] | None = None,
) -> Generator[SparkSession, None, None]:
    config = config or {}
    spark_spession: SparkSession | None = None
    try:
        builder = SparkSession.builder
        builder.enableHiveSupport()
        builder
        builder.appName("local_spark")
        builder.master("local[*]")
        for key, value in config.items():
            builder.config(key, value)

        spark_spession = builder.getOrCreate()
        yield spark_spession
    finally:
        if spark_spession is not None:
            spark_spession.stop()
            spark_spession = None


class BlobReader(DataSourceReader):
    def __init__(self, options: dict[str, str]) -> None:
        self.filename = options["filename"]
        self.batch_size = options.get("batch_size", 100)

    def partitions(self) -> list[InputPartition]:
        return [InputPartition(None)]

    def read(self, partition: InputPartition) -> Iterator[tuple[str, bytes]]:
        if partition.value is not None:
            return

        schema = pa.schema([("type", pa.string()), ("data", pa.binary())])

        with open(self.filename, "rb") as fin:
            for batch in itertools.batched(read_blobs(fin), self.batch_size):
                types = pa.array([blob.header.type for blob in batch], type=pa.string())
                data = pa.array([blob.data for blob in batch], type=pa.binary())
                record = pa.RecordBatch.from_arrays([types, data], schema=schema)
                yield record


class BlobStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, options: dict[str, str]) -> None:
        self.filename = options["filename"]
        self.batch_size = options.get("batch_size", 100)

    def initialOffset(self) -> dict[str, int]:
        return self._make_start(0)

    def _get_offset(self, start: dict[str, int]) -> int:
        return start["offset"]

    def _make_start(self, offset: int) -> dict[str, int]:
        return {"offset": offset}

    def read(self, start: dict[str, int]) -> tuple[Iterator[tuple[str]], dict[str, int]]:
        blobs, offset = self.read_n_at(self.batch_size, self._get_offset(start))
        return [(blob.header.type, blob.data) for blob in blobs], self._make_start(offset)

    def readBetweenOffsets(self, start: dict[str, int], end: dict[str, int]) -> Iterator[tuple[str]]:
        blobs, offset = self.read_between(self._get_offset(start), self._get_offset(end))
        return [(blob.header.type, blob.data) for blob in blobs], self._make_start(offset)

    def read_n_at(self, n: int, offset: int) -> tuple[list[BlobData], int]:
        with open(self.filename, "rb") as fin:
            fin.seek(offset)
            blobs = []
            for _ in range(n):
                blob = read_blob_data(fin)
                if blob is None:
                    break
                blobs.append(blob)
            return blobs, fin.tell()

    def read_between(self, start_offset: int, end_offset: int) -> tuple[list[BlobData], int]:
        with open(self.filename, "rb") as fin:
            fin.seek(start_offset)
            blobs = []
            while fin.tell() < end_offset:
                blob = read_blob_data(fin)
                if blob is None:
                    break
                blobs.append(blob)
            return blobs, fin.tell()


class BlobDataSource(DataSource):
    @classmethod
    def name(cls):
        return "osmpbf"

    def schema(self) -> StructType:
        return StructType(
            [
                StructField("type", StringType()),
                StructField("data", BinaryType()),
            ]
        )

    def reader(self, schema: StructType) -> BlobReader:
        return BlobReader(self.options)

    def simpleStreamReader(self, schema: StructType) -> BlobStreamReader:
        return BlobStreamReader(self.options)
