import sys
import os

def run_ingest():
    print("Running ingest pipeline...")
    os.system("python ingest/extract_sacct.py --configfile ingest/pipeline.toml --log-level INFO")

def run_report():
    print("Generating weekly report...")
    os.system("python reports/weekly_summary.py > reports/latest_summary.md")
    print("Summary saved to reports/latest_summary.md")

def show_help():
    print("""
Usage:
    python pipeline_runner.py ingest     # Run sacct ingest pipeline
    python pipeline_runner.py report     # Generate weekly report
""")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "ingest":
        run_ingest()
    elif cmd == "report":
        run_report()
    else:
        print(f"Unknown command: {cmd}")
        show_help()
