#!/usr/bin/env python3
"""
Download AIS data from NOAA Marine Cadastre.
Downloads zip files and extracts them, skipping if already downloaded.
"""

import os
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm


def download_ais(url: str, data_dir: str = "data") -> Path:
    """
    Download AIS zip file from NOAA and extract it.

    Args:
        url: URL to the AIS zip file
        data_dir: Directory to store downloaded files

    Returns:
        Path to the extracted CSV file
    """
    data_path = Path(data_dir)
    data_path.mkdir(exist_ok=True)

    # Get filename from URL
    filename = os.path.basename(urlparse(url).path)
    zip_path = data_path / filename
    csv_filename = filename.replace(".zip", ".csv")
    csv_path = data_path / csv_filename

    # Check if CSV already exists (already downloaded and extracted)
    if csv_path.exists():
        print(f"CSV already exists: {csv_path}")
        return csv_path

    # Check if zip already exists (downloaded but not extracted)
    if not zip_path.exists():
        print(f"Downloading {url}...")

        # Stream download with progress bar
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(zip_path, "wb") as f:
            with tqdm(total=total_size, unit="B", unit_scale=True, desc=filename) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

        print(f"Downloaded: {zip_path}")
    else:
        print(f"Zip already exists: {zip_path}")

    # Extract the zip file
    print(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(data_path)

    print(f"Extracted: {csv_path}")
    return csv_path


if __name__ == "__main__":
    # Default URL for testing
    default_url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2022/AIS_2022_07_01.zip"

    import sys

    url = sys.argv[1] if len(sys.argv) > 1 else default_url

    csv_path = download_ais(url)
    print(f"\nReady for processing: {csv_path}")
