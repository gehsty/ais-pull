#!/usr/bin/env python3
"""
Process AIS data: filter by wind farm lease polygons and convert to GeoPackage.
Uses polars for memory-efficient CSV processing, then spatial join with lease boundaries.
"""

from functools import lru_cache
from pathlib import Path

import geopandas as gpd
import polars as pl
from shapely.geometry import Point

from config import EXCLUDED_VESSEL_TYPES, LEASE_GEOJSON


@lru_cache(maxsize=1)
def load_lease_boundaries() -> gpd.GeoDataFrame:
    """
    Load and cache the wind farm lease boundaries.
    Reprojects from EPSG:3857 to EPSG:4326 to match AIS data.
    """
    gdf = gpd.read_file(LEASE_GEOJSON)
    # Reproject to WGS84 (same as AIS data)
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


def get_lease_bounds() -> dict:
    """
    Get the overall bounding box of all lease areas for pre-filtering.
    This allows fast rejection of points far outside any lease area.
    """
    gdf = load_lease_boundaries()
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    return {
        "min_lon": bounds[0],
        "min_lat": bounds[1],
        "max_lon": bounds[2],
        "max_lat": bounds[3],
    }


def process_ais(
    csv_path: str,
    output_dir: str = "output",
    output_name: str | None = None,
) -> Path | None:
    """
    Filter AIS data by wind farm lease boundaries and save as GeoPackage.

    Args:
        csv_path: Path to the AIS CSV file
        output_dir: Directory for output files
        output_name: Output filename (without extension)

    Returns:
        Path to the output GeoPackage file, or None if no data found
    """
    csv_path = Path(csv_path)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    if output_name is None:
        output_name = f"ais_{csv_path.stem.replace('AIS_', '')}"

    gpkg_path = output_path / f"{output_name}.gpkg"

    print(f"Loading CSV: {csv_path}")

    # Load lease boundaries
    leases = load_lease_boundaries()
    print(f"Loaded {len(leases)} lease boundaries")

    # Get overall bounds for pre-filtering (fast rejection)
    bounds = get_lease_bounds()
    print(f"Pre-filter bounds: {bounds}")

    # Read CSV with polars (memory efficient)
    # Pre-filter to overall bounding box first (fast numerical filter)
    df = (
        pl.scan_csv(csv_path)
        .filter(
            # Valid coordinates
            (pl.col("LAT") != 91)
            & (pl.col("LON") != 181)
            # Exclude non-work vessels
            & ~pl.col("VesselType").is_in(EXCLUDED_VESSEL_TYPES)
            # Pre-filter to overall bounds (fast rejection)
            & (pl.col("LAT") >= bounds["min_lat"])
            & (pl.col("LAT") <= bounds["max_lat"])
            & (pl.col("LON") >= bounds["min_lon"])
            & (pl.col("LON") <= bounds["max_lon"])
        )
        .collect()
    )

    print(f"Pre-filtered to {len(df):,} rows within overall bounds")

    if len(df) == 0:
        print("Warning: No data found within bounds!")
        return None

    # Convert to GeoDataFrame for spatial join
    print("Creating geometry...")
    pdf = df.to_pandas()
    geometry = [Point(lon, lat) for lon, lat in zip(pdf["LON"], pdf["LAT"], strict=True)]
    ais_gdf = gpd.GeoDataFrame(pdf, geometry=geometry, crs="EPSG:4326")

    # Spatial join - keep only points within lease areas
    print("Performing spatial join with lease boundaries...")
    result = gpd.sjoin(
        ais_gdf, leases[["geometry", "LEASE_NUMBER"]], how="inner", predicate="within"
    )

    # Drop the index_right column from sjoin
    if "index_right" in result.columns:
        result = result.drop(columns=["index_right"])

    print(f"Filtered to {len(result):,} rows within lease boundaries")

    if len(result) == 0:
        print("Warning: No data found within lease boundaries!")
        return None

    # Save as GeoPackage
    print(f"Saving GeoPackage: {gpkg_path}")
    result.to_file(gpkg_path, driver="GPKG")

    print(f"Done! Output: {gpkg_path}")
    print(f"File size: {gpkg_path.stat().st_size / 1024 / 1024:.2f} MB")

    return gpkg_path


if __name__ == "__main__":
    import sys

    # Default to the expected CSV location
    default_csv = "data/AIS_2022_07_01.csv"

    csv_path = sys.argv[1] if len(sys.argv) > 1 else default_csv

    if not Path(csv_path).exists():
        print(f"Error: CSV not found at {csv_path}")
        print("Run download_ais.py first to download the data.")
        sys.exit(1)

    process_ais(csv_path)
