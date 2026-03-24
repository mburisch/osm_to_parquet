from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import TypeVar

import click

from osmpq.blobs import pbf_to_blob_parquet
from osmpq.elements import pbf_parquet_to_elements_parquet
from osmpq.elements import pbf_to_elements_parquet
from osmpq.io import WriterConfig

CommandFn = TypeVar("CommandFn", bound=Callable[..., Any])


def create_writer_config(
    max_rows_per_row_group: int,
    max_rows_per_file: int | None,
    max_file_size_mb: int | None,
) -> WriterConfig:
    return WriterConfig(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_bytes=_mb_to_bytes(max_file_size_mb),
    )


def _mb_to_bytes(size_mb: int | None) -> int | None:
    if size_mb is None:
        return None
    return size_mb * 1024 * 1024


def common_options(function: CommandFn) -> CommandFn:
    options = [
        click.option(
            "--pbf-filename",
            required=True,
            type=str,
            help="Input OSM PBF filename or URI.",
        ),
        click.option(
            "--output-path",
            required=True,
            type=str,
            help="Output directory path or URI.",
        ),
        click.option(
            "--header-output-filename",
            type=str,
            default=None,
            help="Optional filename for the extracted OSM header JSON.",
        ),
        click.option(
            "--max-rows-per-row-group",
            type=click.IntRange(min=1),
            default=None,
            help="Maximum rows per Parquet row group.",
        ),
        click.option(
            "--max-rows-per-file",
            type=click.IntRange(min=1),
            default=None,
            help="Optional maximum rows written to each Parquet file.",
        ),
        click.option(
            "--max-file-size-mb",
            type=click.IntRange(min=1),
            default=128,
            show_default=True,
            help="Maximum Parquet file size in MiB.",
        ),
    ]
    for option in reversed(options):
        function = option(function)
    return function


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main() -> None:
    """Convert OSM PBF data into Parquet outputs."""


@main.command(name="blobs")
@common_options
def blobs_command(
    pbf_filename: str,
    output_path: str,
    header_output_filename: str | None,
    max_rows_per_row_group: int,
    max_rows_per_file: int | None,
    max_file_size_mb: int,
) -> None:
    writer_config = create_writer_config(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_mb=max_file_size_mb,
    )
    pbf_to_blob_parquet(pbf_filename, output_path, header_output_filename, writer_config)


@main.command(name="osm-elements")
@common_options
def osm_elements_command(
    pbf_filename: str,
    output_path: str,
    header_output_filename: str | None,
    max_rows_per_row_group: int,
    max_rows_per_file: int | None,
    max_file_size_mb: int,
) -> None:
    writer_config = create_writer_config(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_mb=max_file_size_mb,
    )
    pbf_to_elements_parquet(pbf_filename, output_path, header_output_filename, writer_config)


@main.command(name="parquet-elements")
@common_options
def parquet_elements_command(
    blob_parquet_filename: str,
    output_path: str,
    max_rows_per_row_group: int,
    max_rows_per_file: int | None,
    max_file_size_mb: int,
) -> None:
    writer_config = create_writer_config(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_mb=max_file_size_mb,
    )
    pbf_parquet_to_elements_parquet(blob_parquet_filename, output_path, writer_config)


if __name__ == "__main__":
    main()
