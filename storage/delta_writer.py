import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

DATA_ROOT = "storage/delta"

def clean_and_normalize(records):
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Add date partition column (fallback to today if JobID is missing)
    try:
        df['date'] = pd.to_datetime(df['JobID'].str.extract(r'(\d+)')[0], unit='s', errors='coerce').dt.date
    except:
        df['date'] = datetime.utcnow().date()

    # Coerce types
    for col in ['TotalCPU', 'NCPUS']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def write_parquet(df, batch_id=None):
    if df.empty:
        return

    date_str = df['date'].iloc[0].strftime("%Y-%m-%d")
    partition_dir = os.path.join(DATA_ROOT, f"date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)

    fname = f"sacct_batch_{batch_id or int(datetime.now().timestamp())}.parquet"
    path = os.path.join(partition_dir, fname)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    print(f"Wrote {len(df)} records to {path}")
