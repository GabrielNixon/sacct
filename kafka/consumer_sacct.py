import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

INPUT_PATH = "data/run/version=v1/sacct.ndjson"
OUTPUT_BASE = "storage/delta"

def load_ndjson(path):
    with open(path, "r") as f:
        for line in f:
            yield json.loads(line)

def write_partitioned_parquet(records):
    df = pd.DataFrame(records)
    if df.empty:
        print("No records found.")
        return

    # You can use another field like 'Submit' or 'Start' if JobID isn't timestamp-based
    if 'Submit' in df.columns:
        df['timestamp'] = pd.to_datetime(df['Submit'], errors='coerce')
    else:
        df['timestamp'] = pd.Timestamp.utcnow()  # fallback if needed

    df['date'] = df['timestamp'].dt.date.astype(str)

    for date, group in df.groupby("date"):
        output_dir = os.path.join(OUTPUT_BASE, f"date={date}")
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"part-{datetime.utcnow().timestamp():.0f}.parquet")
        group.drop(columns=["date", "timestamp"], errors="ignore").to_parquet(file_path)
        print(f" Wrote {len(group)} records to {file_path}")

def main():
    print(f" Reading from {INPUT_PATH}...")
    records = list(load_ndjson(INPUT_PATH))
    write_partitioned_parquet(records)

if __name__ == "__main__":
    main()

