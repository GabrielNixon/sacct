import subprocess
import json
import toml
import time
from datetime import datetime
from kafka import KafkaProducer
from pathlib import Path

def load_config(path):
    return toml.load(path)

def run_sacct(start_ts, end_ts, fields, states, delimiter):
    start_fmt = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%dT%H:%M:%S")
    end_fmt = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%dT%H:%M:%S")
    cmd = [
        "sacct",
        "--duplicate",
        "--allusers",
        "--noconvert",
        "--parsable2",
        "--delimiter", delimiter,
        "--format", ",".join(fields),
        "--starttime", start_fmt,
        "--endtime", end_fmt,
        "--state", ",".join(states)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip().split("\n")

def parse_records(lines, fields, delimiter):
    return [dict(zip(fields, line.split(delimiter))) for line in lines if line.strip()]

def send_to_kafka(producer, topic, records):
    for rec in records:
        producer.send(topic, json.dumps(rec).encode("utf-8"))
    producer.flush()

def main():
    config = load_config("ingest/pipeline.toml")
    meta = config["meta"]
    storage = config["storage"]
    sacct_cfg = config["sacct"]

    # Tracking file for NEXT_START
    prefix = Path(storage["prefix"])
    version = meta["version"]
    next_start_path = prefix / f"NEXT_START_{version}.txt"
    prefix.mkdir(parents=True, exist_ok=True)

    if next_start_path.exists():
        start_ts = int(next_start_path.read_text().strip())
    else:
        start_ts = meta["start"]
        next_start_path.write_text(str(start_ts))

    end = meta["end"]
    end_ts = int(time.time()) if end == "now" else int(end)

    window = sacct_cfg["window"]
    fields = sacct_cfg["fields"]
    states = sacct_cfg["states"]
    delimiter = sacct_cfg["delimiter"]

    producer = KafkaProducer(bootstrap_servers="localhost:9092")  # adjust if needed

    while start_ts < end_ts:
        chunk_end = min(start_ts + window, end_ts)
        print(f"Processing {start_ts} to {chunk_end}...")
        try:
            lines = run_sacct(start_ts, chunk_end, fields, states, delimiter)
            records = parse_records(lines, fields, delimiter)
            send_to_kafka(producer, "sacct-raw", records)
            print(f"Sent {len(records)} records to Kafka")
            next_start_path.write_text(str(chunk_end + 1))
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            break
        start_ts = chunk_end + 1

if __name__ == "__main__":
    main()
