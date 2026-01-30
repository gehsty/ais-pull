#!/usr/bin/env python3
"""
Plot AIS data from GeoParquet file.
"""

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt


def plot_ais(parquet_path: str, output_path: str | None = None) -> None:
    """
    Create a plot of AIS vessel positions.

    Args:
        parquet_path: Path to the GeoParquet file
        output_path: Optional path to save the plot (shows interactive if None)
    """
    gdf = gpd.read_parquet(parquet_path)

    fig, ax = plt.subplots(figsize=(12, 10))

    # Plot vessel positions colored by vessel type
    scatter = ax.scatter(
        gdf.geometry.x,
        gdf.geometry.y,
        c=gdf["VesselType"],
        cmap="tab20",
        alpha=0.6,
        s=20,
    )

    # Add colorbar
    plt.colorbar(scatter, ax=ax, label="Vessel Type")

    # Labels and title
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"AIS Vessel Positions - South Fork Wind Farm Area\n{len(gdf):,} positions")

    # Add grid
    ax.grid(True, alpha=0.3)

    # Equal aspect ratio for geographic data
    ax.set_aspect("equal")

    # Add some stats as text
    unique_vessels = gdf["MMSI"].nunique()
    unique_names = gdf["VesselName"].nunique()
    stats_text = f"Unique vessels (MMSI): {unique_vessels}\nUnique names: {unique_names}"
    ax.text(
        0.02,
        0.98,
        stats_text,
        transform=ax.transAxes,
        verticalalignment="top",
        fontsize=9,
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")
    else:
        plt.show()


if __name__ == "__main__":
    import sys

    default_parquet = "output/southfork_AIS_2022_07_01.parquet"
    parquet_path = sys.argv[1] if len(sys.argv) > 1 else default_parquet

    if not Path(parquet_path).exists():
        print(f"Error: Parquet file not found at {parquet_path}")
        sys.exit(1)

    # Save to file by default (for headless environments)
    output_file = "output/ais_plot.png"
    plot_ais(parquet_path, output_file)
