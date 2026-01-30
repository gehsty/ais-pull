"""
Configuration for AIS data processing.
Wind farm lease boundaries and vessel type filters.
"""

from pathlib import Path

# Wind farm lease boundaries (BOEM geojson)
LEASE_GEOJSON = (
    Path(__file__).parent / "geojson" / "Wind_Lease_Boundaries__BOEM__2752026592552254440.geojson"
)

# Vessel types to EXCLUDE (not relevant to offshore wind operations)
# 30 = Fishing, 36 = Sailing, 37 = Pleasure craft, 60-69 = Passenger ships
EXCLUDED_VESSEL_TYPES = [30, 36, 37, *range(60, 70)]

# AIS data URL template
AIS_URL_TEMPLATE = (
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{year}_{month:02d}_{day:02d}.zip"
)

# Date range for processing
START_DATE = "2022-01-01"
END_DATE = "2025-12-31"
