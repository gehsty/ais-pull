"""
Merge daily GeoPackage files into a single GeoParquet file.
Optimized for PostGIS upload via ogr2ogr or geopandas.
"""

import argparse
import gc
import re
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from tqdm import tqdm


def get_gpkg_files(
    output_dir: Path,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[Path]:
    """
    Get list of GeoPackage files, optionally filtered by date range.

    Args:
        output_dir: Directory containing gpkg files
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Sorted list of Path objects for gpkg files
    """
    pattern = re.compile(r"ais_(\d{4})_(\d{2})_(\d{2})\.gpkg$")
    files = []

    start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    for f in output_dir.glob("ais_*.gpkg"):
        match = pattern.match(f.name)
        if match:
            year, month, day = map(int, match.groups())
            file_date = datetime(year, month, day).date()

            if start and file_date < start:
                continue
            if end and file_date > end:
                continue

            files.append(f)

    return sorted(files)


def iter_gdfs(
    gpkg_files: list[Path],
    batch_size: int = 10,
) -> Iterator[gpd.GeoDataFrame]:
    """
    Lazily iterate over GeoDataFrames, yielding combined batches.

    Memory-efficient: loads `batch_size` files at a time, concatenates,
    and yields. This keeps memory usage bounded.

    Args:
        gpkg_files: List of GeoPackage file paths
        batch_size: Number of files to load per batch

    Yields:
        GeoDataFrame for each batch
    """
    for i in range(0, len(gpkg_files), batch_size):
        batch_files = gpkg_files[i : i + batch_size]
        gdfs = [gpd.read_file(f) for f in batch_files]
        combined = pd.concat(gdfs, ignore_index=True)
        # Ensure it's a GeoDataFrame with proper CRS
        combined = gpd.GeoDataFrame(combined, crs="EPSG:4326")
        yield combined
        # Explicit cleanup
        del gdfs
        del combined
        gc.collect()


def merge_to_parquet(
    gpkg_files: list[Path],
    output_path: Path,
    batch_size: int = 10,
    show_progress: bool = True,
) -> tuple[Path, int]:
    """
    Merge GeoPackage files to GeoParquet format.

    Uses geopandas native GeoParquet support for proper geometry encoding.

    Args:
        gpkg_files: List of GeoPackage file paths
        output_path: Output parquet file path
        batch_size: Files to process per batch (for progress display)
        show_progress: Show tqdm progress bar

    Returns:
        Tuple of (output path, total row count)
    """
    iterator = gpkg_files
    if show_progress:
        iterator = tqdm(gpkg_files, desc="Reading files")

    # Read all files
    gdfs = [gpd.read_file(f) for f in iterator]
    total_rows = sum(len(gdf) for gdf in gdfs)

    if show_progress:
        print("Concatenating and writing parquet...")

    # Concatenate and write
    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, crs="EPSG:4326")
    combined.to_parquet(output_path)

    # Cleanup
    del gdfs
    del combined
    gc.collect()

    return output_path, total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Merge GeoPackage files into a single GeoParquet file"
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        type=Path,
        default=Path("output"),
        help="Input directory containing gpkg files (default: output)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Output parquet file path",
    )
    parser.add_argument(
        "--start-date",
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=10,
        help="Files to process per batch (default: 10)",
    )

    args = parser.parse_args()

    # Get files
    gpkg_files = get_gpkg_files(args.input_dir, args.start_date, args.end_date)

    if not gpkg_files:
        print("No GeoPackage files found matching criteria")
        return 1

    print(f"Found {len(gpkg_files)} GeoPackage files")
    if args.start_date or args.end_date:
        print(f"Date range: {args.start_date or 'start'} to {args.end_date or 'end'}")

    # Merge
    output_path, total_rows = merge_to_parquet(
        gpkg_files,
        args.output,
        batch_size=args.batch_size,
    )

    # Report
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nMerge complete!")
    print(f"  Output: {output_path}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  File size: {file_size_mb:.1f} MB")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
