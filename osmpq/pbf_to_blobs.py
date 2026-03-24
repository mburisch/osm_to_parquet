from __future__ import annotations

import argparse

from osmpq.arrow import WriterConfig
from osmpq.blobs import pbf_to_blob_parquet


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert OSM PBF to Parquet")
    parser.add_argument("--pbf-filename", type=str, help="Path to the OSM PBF file")
    parser.add_argument("--output-path", type=str, help="Path to the output directory")
    parser.add_argument("--header-output-filename", type=str, help="Path to the output header file")
    parser.add_argument("--max-file-size-mb", type=int, default=128, help="Maximum file size in MB")
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    writer_config = WriterConfig(
        max_row_group_size=16,
        max_file_size_bytes=args.max_file_size_mb * 1024 * 1024,
    )

    pbf_to_blob_parquet(args.pbf_filename, args.output_path, args.header_output_filename, writer_config)


if __name__ == "__main__":
    main()
