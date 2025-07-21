import time
import json
import toml
import logging
import dateparser
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import argparse

logging.basicConfig()
log = logging.getLogger("extractor")


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

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"sacct failed: {result.stderr.strip()}")
    
    lines = result.stdout.strip().split("\n")
    return [line.split(delimiter) for line in lines if line]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configfile", required=True)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    log.setLevel(args.log_level.upper())
    config = load_config(args.configfile)

    meta = config["meta"]
    store = config["storage"]
    sacct_cfg = config["sacct"]

    # Parse start and end from TOML (support "now", "now-7days", etc.)
    start_val = meta["start"]
    end_val = meta["end"]

    if isinstance(start_val, str):
        start_ts = int(dateparser.parse(start_val).timestamp())
    else:
        start_ts = int(start_val)

    if isinstance(end_val, str):
        if end_val.lower() == "now":
            end_ts = int(time.time())
        else:
            end_ts = int(dateparser.parse(end_val).timestamp())
    else:
        end_ts = int(end_val)

    version = meta.get("version", "v1")
    outdir = Path(store.get("prefix", "./data")) / "run" / f"version={version}"
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / "sacct.ndjson"
    statefile = outdir / "NEXT_START"

    if statefile.exists():
        with open(statefile) as f:
            start_ts = int(f.read().strip())

    window = int(sacct_cfg.get("window", 300))
    fields = sacct_cfg["fields"]
    states = sacct_cfg["states"]
    delimiter = sacct_cfg.get("delimiter", "|")

    log.info(f"Saving to: {outfile}")
    log.info(f"Starting at: {datetime.fromtimestamp(start_ts)}")

    with open(outfile, "a") as f:
        while start_ts < end_ts:
            next_ts = start_ts + window
            log.info(f"Processing {start_ts} to {next_ts}...")
            try:
                rows = run_sacct(start_ts, next_ts, fields, states, delimiter)
                for row in rows:
                    data = dict(zip(fields, row))
                    json.dump(data, f)
                    f.write("\n")
                log.info(f"Wrote {len(rows)} records")
            except Exception as e:
                log.warning(f"Skipping window due to error: {e}")
            start_ts = next_ts
            with open(statefile, "w") as s:
                s.write(str(start_ts))


if __name__ == "__main__":
    main()

