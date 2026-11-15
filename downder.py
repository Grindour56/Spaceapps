
import os
import time
from io import StringIO
from pathlib import Path
from datetime import date, timedelta

import requests
import pandas as pd

# ============== CONFIGURATION ==============

KEY = os.environ.get("FIRMS_MAP_KEY")

START_YEAR = 2000
END_YEAR = 2017

SOURCES = [
    "MODIS_SP",
    "VIIRS_SNPP_SP",
]

# Stop downloading the current year when remaining quota
# falls below this number.
MIN_REMAINING = 100

OUTPUT = Path("firms_historical")
OUTPUT.mkdir(exist_ok=True)

BASE = "https://firms.modaps.eosdis.nasa.gov"

session = requests.Session()

if not KEY:
    raise RuntimeError("FIRMS_MAP_KEY is not set")


# ============== API STATUS ==============

def get_status():
    url = f"{BASE}/mapserver/mapkey_status/"

    response = session.get(
        url,
        params={"MAP_KEY": KEY},
        timeout=30
    )
    response.raise_for_status()

    return response.json()


def print_status(label="API STATUS"):
    status = get_status()

    limit = int(status["transaction_limit"])
    used = int(status["current_transactions"])
    remaining = max(0, limit - used)

    print(f"\n--- {label} ---")
    print(f"Used:      {used:,}")
    print(f"Limit:     {limit:,}")
    print(f"Remaining: {remaining:,}")
    print(f"Interval:  {status['transaction_interval']}")

    return used, limit, remaining


# ============== AVAILABILITY ==============

def get_availability():
    url = f"{BASE}/api/data_availability/csv/{KEY}/all"

    response = session.get(url, timeout=60)
    response.raise_for_status()

    return pd.read_csv(StringIO(response.text))


# ============== DOWNLOAD ==============

def download_chunk(source, start, days):
    url = (
        f"{BASE}/api/area/csv/"
        f"{KEY}/{source}/world/{days}/{start.isoformat()}"
    )

    response = session.get(url, timeout=300)
    response.raise_for_status()

    if response.text.lstrip().startswith("<"):
        raise RuntimeError("Unexpected HTML response")

    return response.text


def download_year(source, year, available_start, available_end):
    folder = OUTPUT / source / str(year)
    folder.mkdir(parents=True, exist_ok=True)

    start = max(date(year, 1, 1), available_start)
    end = min(date(year, 12, 31), available_end)

    if start > end:
        print(f"No available dates for {source} in {year}")
        return True

    print(f"\n{'=' * 55}")
    print(f"SOURCE: {source} | YEAR: {year}")
    print(f"DATE RANGE: {start} to {end}")
    print("=" * 55)

    used_before, limit, remaining = print_status(
        "BEFORE YEAR"
    )

    current = start

    while current <= end:

        used, limit, remaining = print_status(
            f"BEFORE {current}"
        )

        if remaining < MIN_REMAINING:
            print("\nQuota too low. Pausing this year.")
            return False

        days = min(5, (end - current).days + 1)
        chunk_end = current + timedelta(days=days - 1)

        filename = (
            f"{source}_{current}_{chunk_end}.csv"
        )

        filepath = folder / filename

        if filepath.exists():
            print(f"Already downloaded: {filename}")
            current += timedelta(days=days)
            continue

        print(f"\nDownloading {filename}")

        try:
            csv_text = download_chunk(source, current, days)

            df = pd.read_csv(StringIO(csv_text))

            df.to_csv(filepath, index=False)

            print(f"Saved {len(df):,} records")

        except Exception as error:
            print(f"Download failed: {error}")
            print("Rerun the script to resume.")
            return False

        used, limit, remaining = print_status(
            "AFTER CHUNK"
        )

        current += timedelta(days=days)

        time.sleep(0.5)

    used_after, limit, remaining = print_status(
        f"AFTER YEAR {year}"
    )

    print(f"\n{year} completed for {source}.")
    print(f"Transactions consumed: {used_after - used_before}")

    return True


# ============== MAIN ==============

def main():
    availability = get_availability()

    print_status("INITIAL API STATUS")

    for source in SOURCES:

        match = availability[
            availability["data_id"] == source
        ]

        if match.empty:
            print(f"\n{source} not available.")
            continue

        available_start = date.fromisoformat(
            str(match.iloc[0]["min_date"])[:10]
        )

        available_end = date.fromisoformat(
            str(match.iloc[0]["max_date"])[:10]
        )

        print(f"\nStarting source: {source}")

        for year in range(START_YEAR, END_YEAR + 1):

            success = download_year(
                source,
                year,
                available_start,
                available_end
            )

            if not success:
                print(
                    f"\nPaused at {source}, year {year}."
                )
                print(
                    "Run again after quota resets. "
                    "Existing chunks will be skipped."
                )
                return

    print("\nAll historical years completed!")


if __name__ == "__main__":
    main()

