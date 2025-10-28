from __future__ import annotations

import argparse

from osmpq.parquet import WriterConfig
from osmpq.pbf_converter import pbf_to_blobs


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert OSM PBF to Parquet")
    parser.add_argument("--pbf_filename", type=str, help="Path to the OSM PBF file")
    parser.add_argument("--output_path", type=str, help="Path to the output directory")
    parser.add_argument("--max_file_size_mb", type=int, default=128, help="Maximum file size in MB")
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    writer_config = WriterConfig(
        max_rows_per_group=16,
        max_file_size=args.max_file_size_mb * 1024 * 1024,
    )

    pbf_to_blobs(args.pbf_filename, args.output_path, writer_config)


if __name__ == "__main__":
    main()
