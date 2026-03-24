from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import TypeVar
from typing import overload

import click

from osmpq.io import clear_output_path
from osmpq.split import split_pbf

CommandFn = TypeVar("CommandFn", bound=Callable[..., Any])


@overload
def _mb_to_bytes(size_mb: int) -> int: ...


@overload
def _mb_to_bytes(size_mb: None) -> None: ...


def _mb_to_bytes(size_mb: int | None) -> int | None:
    if size_mb is None:
        return None
    return size_mb * 1024 * 1024


def writer_options(function: CommandFn) -> CommandFn:
    options = [
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


def split_options(function: CommandFn) -> CommandFn:
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
            "--max-file-size-mb",
            type=click.IntRange(min=1),
            default=128,
            show_default=True,
            help="Maximum size of each split PBF blob file in MiB.",
        ),
    ]
    for option in reversed(options):
        function = option(function)
    return function


def pbf_input_options(function: CommandFn) -> CommandFn:
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
    ]
    for option in reversed(options):
        function = option(function)
    return function


def build_writer_config(
    *,
    max_rows_per_row_group: int | None,
    max_rows_per_file: int | None,
    max_file_size_mb: int,
) -> Any:
    from osmpq.io import WriterConfig

    return WriterConfig(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_bytes=_mb_to_bytes(max_file_size_mb),
    )


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main() -> None:
    """Split OSM PBF data and convert it into Parquet outputs."""


@main.command(name="clear-output-path")
@click.option("--output-path", required=True, type=str, help="Output directory path or URI.")
def clear_output_path_command(output_path: str) -> None:
    """Clear the output directory path."""
    clear_output_path(output_path)


@main.command(name="split")
@split_options
def split_command(
    pbf_filename: str,
    output_path: str,
    max_file_size_mb: int,
) -> None:
    """Split an OSM PBF into smaller blob PBF files."""
    clear_output_path(output_path)
    split_pbf(
        pbf_filename=pbf_filename,
        output_folder=output_path,
        max_file_size_bytes=_mb_to_bytes(max_file_size_mb or 128),
    )


@main.command(name="elements")
@pbf_input_options
@writer_options
def osm_elements_command(
    pbf_filename: str,
    output_path: str,
    header_output_filename: str | None,
    max_rows_per_row_group: int | None,
    max_rows_per_file: int | None,
    max_file_size_mb: int,
) -> None:
    """Convert OSM PBF data into elements (nodes, ways, relations) Parquet files."""
    from osmpq.elements import pbf_to_elements_parquet

    writer_config = build_writer_config(
        max_rows_per_row_group=max_rows_per_row_group,
        max_rows_per_file=max_rows_per_file,
        max_file_size_mb=max_file_size_mb,
    )
    pbf_to_elements_parquet(pbf_filename, output_path, header_output_filename, writer_config)


if __name__ == "__main__":
    main()
