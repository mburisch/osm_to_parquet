from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from osmpq.element_converter import blobs_to_elements
from osmpq.parquet import WriterConfig


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert OSM blob parquet to element parquet")
    parser.add_argument("--input_path", type=str, help="Path to the OSM PBF source parquets")
    parser.add_argument("--output_path", type=str, help="Path to the output directory")
    parser.add_argument("--max_file_size_mb", type=int, default=128, help="Maximum file size in MB")
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    config = WriterConfig(max_file_size=args.max_file_size_mb * 1024 * 1024)

    with SparkSession.builder.getOrCreate() as spark_session:
        blobs = spark_session.read.parquet(args.input_path)
        blobs_to_elements(blobs, config, args.output_path)


if __name__ == "__main__":
    main()
