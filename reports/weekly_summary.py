# -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template

DELTA_DIR = "storage/delta"
TEMPLATE_PATH = "reports/templates/summary_prompt.jinja"
OUTPUT_PATH = "reports/weekly_summary.md"

def load_last_7_days():
    today = datetime.utcnow().date()
    date_dirs = []

    for folder in os.listdir(DELTA_DIR):
        if folder.startswith("date="):
            date_str = folder.split("=")[-1]
            try:
                folder_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if today - timedelta(days=7) <= folder_date <= today:
                    date_dirs.append(os.path.join(DELTA_DIR, folder))
            except:
                continue

    dfs = []
    for ddir in date_dirs:
        for fname in os.listdir(ddir):
            if fname.endswith(".parquet"):
                dfs.append(pd.read_parquet(os.path.join(ddir, fname)))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def summarize_jobs(df):
    summary = {}

    if df.empty:
        return {"users": [], "total_jobs": 0}

    df['JobID'] = df['JobID'].astype(str)

    summary["total_jobs"] = len(df)
    summary["by_user"] = df.groupby("User")["JobID"].count().to_dict()
    summary["by_partition"] = df.groupby("Partition")["JobID"].count().to_dict()
    summary["by_state"] = df.groupby("State")["JobID"].count().to_dict()

    df["Elapsed"] = pd.to_timedelta(df["Elapsed"], errors='coerce')
    # Clean and convert columns
    df["Elapsed"] = pd.to_timedelta(df["Elapsed"], errors='coerce')
    df["NCPUS"] = pd.to_numeric(df["NCPUS"], errors="coerce").fillna(1)

# Compute CPU hours safely
    df["CPUHours"] = (df["Elapsed"].dt.total_seconds() / 3600) * df["NCPUS"]


    usage = df.groupby("User").agg({
        "JobID": "count",
        "CPUHours": "sum"
    }).reset_index()

    usage["CPUHours"] = usage["CPUHours"].round(2)

    summary["users"] = usage.to_dict(orient="records")
    return summary

def render_template(summary):
    with open(TEMPLATE_PATH) as f:
        template = Template(f.read())
    return template.render(summary=summary, generated=datetime.utcnow().isoformat())

def save_report(markdown):
    os.makedirs("reports", exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write(markdown)
    print("Weekly report saved to", OUTPUT_PATH)

def main():
    df = load_last_7_days()
    summary = summarize_jobs(df)
    markdown = render_template(summary)
    save_report(markdown)

if __name__ == "__main__":
    main()
