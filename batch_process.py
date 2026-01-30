#!/usr/bin/env python3
"""
Batch process AIS data for all days in date range.
Downloads, processes, and cleans up each day sequentially to save disk space.
Tracks progress in a text file for resume capability.
"""

import os
from datetime import date, timedelta
from pathlib import Path

from config import AIS_URL_TEMPLATE, END_DATE, START_DATE
from download_ais import download_ais
from process_ais import process_ais

# Directories
DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
PROGRESS_FILE = Path("progress.txt")


def load_completed_dates() -> set[str]:
    """Load set of already completed dates from progress file."""
    if not PROGRESS_FILE.exists():
        return set()

    with open(PROGRESS_FILE) as f:
        return {line.strip() for line in f if line.strip()}


def mark_completed(date_str: str) -> None:
    """Mark a date as completed in the progress file."""
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"{date_str}\n")


def cleanup_download(date_str: str) -> None:
    """Remove downloaded zip and csv files to save disk space."""
    year, month, day = date_str.split("-")
    zip_name = f"AIS_{year}_{month}_{day}.zip"
    csv_name = f"AIS_{year}_{month}_{day}.csv"

    zip_path = DATA_DIR / zip_name
    csv_path = DATA_DIR / csv_name

    if zip_path.exists():
        os.remove(zip_path)
        print(f"Deleted: {zip_path}")

    if csv_path.exists():
        os.remove(csv_path)
        print(f"Deleted: {csv_path}")


def generate_dates(start: str, end: str):
    """Generate all dates between start and end (inclusive)."""
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def process_date(d: date) -> bool:
    """
    Process a single date: download, filter, save, cleanup.

    Returns True if successful, False otherwise.
    """
    date_str = d.isoformat()
    year, month, day = d.year, d.month, d.day

    print(f"\n{'=' * 60}")
    print(f"Processing: {date_str}")
    print(f"{'=' * 60}")

    # Build URL
    url = AIS_URL_TEMPLATE.format(year=year, month=month, day=day)

    try:
        # Download
        csv_path = download_ais(url, str(DATA_DIR))

        # Process
        output_name = f"ais_{year}_{month:02d}_{day:02d}"
        result = process_ais(str(csv_path), str(OUTPUT_DIR), output_name)

        # Cleanup downloaded files
        cleanup_download(date_str)

        if result is None:
            print(f"No data for {date_str} (empty after filtering)")

        return True

    except Exception as e:
        print(f"ERROR processing {date_str}: {e}")
        # Still cleanup on error
        cleanup_download(date_str)
        return False


def main():
    """Main batch processing loop."""
    print("AIS Batch Processor")
    print(f"Date range: {START_DATE} to {END_DATE}")

    # Create directories
    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Load progress
    completed = load_completed_dates()
    print(f"Already completed: {len(completed)} days")

    # Count total days
    all_dates = list(generate_dates(START_DATE, END_DATE))
    remaining = [d for d in all_dates if d.isoformat() not in completed]

    print(f"Total days: {len(all_dates)}")
    print(f"Remaining: {len(remaining)}")

    if not remaining:
        print("All dates already processed!")
        return

    # Process each date
    success_count = 0
    error_count = 0

    for i, d in enumerate(remaining):
        date_str = d.isoformat()
        print(f"\n[{i + 1}/{len(remaining)}] {date_str}")

        success = process_date(d)

        if success:
            mark_completed(date_str)
            success_count += 1
        else:
            error_count += 1

    print(f"\n{'=' * 60}")
    print("Batch processing complete!")
    print(f"Successful: {success_count}")
    print(f"Errors: {error_count}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
