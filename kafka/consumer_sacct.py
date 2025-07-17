import json
from kafka import KafkaConsumer
import pandas as pd
import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = "storage/delta"

def write_parquet_batch(records, batch_id):
    if not records:
        return

    df = pd.DataFrame(records)

    # Convert timestamps for partitioning (e.g., by day)
    df['date'] = pd.to_datetime(df['JobID'].str.extract(r'(\d+)')[0], errors='coerce', unit='s').dt.date
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(DATA_DIR, f"date={date_str}")
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, f"sacct_batch_{batch_id}.parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    print(f"Wrote {len(df)} records to {file_path}")

def consume():
    consumer = KafkaConsumer(
        "sacct-raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sacct-consumers",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    buffer = []
    batch_size = 1000
    batch_id = 0

    for message in consumer:
        buffer.append(message.value)
        if len(buffer) >= batch_size:
            write_parquet_batch(buffer, batch_id)
            buffer.clear()
            batch_id += 1

if __name__ == "__main__":
    co
