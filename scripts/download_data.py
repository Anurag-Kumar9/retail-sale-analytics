#!/usr/bin/env python3
"""Download UCI Online Retail and convert the workbook to a Spark-friendly CSV."""

from pathlib import Path

import pandas as pd
import requests


DATA_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
WORKBOOK = RAW_DIR / "online_retail.xlsx"
CSV_FILE = RAW_DIR / "online_retail.csv"


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not WORKBOOK.exists():
        print(f"Downloading dataset from {DATA_URL}")
        response = requests.get(DATA_URL, timeout=120)
        response.raise_for_status()
        WORKBOOK.write_bytes(response.content)

    print("Converting Excel workbook to CSV...")
    frame = pd.read_excel(WORKBOOK, engine="openpyxl")
    frame.to_csv(CSV_FILE, index=False)
    print(f"Saved {len(frame):,} rows to {CSV_FILE}")


if __name__ == "__main__":
    main()
