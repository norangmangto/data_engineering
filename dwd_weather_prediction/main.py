import subprocess
import os
import time
import argparse
import sys

def run_command(command, cwd=None):
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, cwd=cwd, check=True)
    return result

def run_pipeline():
    print("\n=== Starting Pipeline Execution ===")

    print("\n[Step 1] Data Ingestion (Incremental)")
    run_command("uv run src/ingestion.py")

    print("\n[Step 2] Data Processing (dbt)")
    run_command("uv run dbt build --profiles-dir .", cwd="transform")

    print("\n[Step 3] Model Training")
    run_command("uv run src/train.py")

    print("\n=== Pipeline Execution Completed ===")

def main():
    parser = argparse.ArgumentParser(description="DWD Weather Prediction Pipeline")
    parser.add_argument("--continuous", action="store_true", help="Run the pipeline continuously")
    parser.add_argument("--interval", type=int, default=86400, help="Interval in seconds for continuous mode (default: 24h)")

    args = parser.parse_args()

    if args.continuous:
        print(f"Starting continuous mode. Pipeline will run every {args.interval} seconds.")
        try:
            while True:
                run_pipeline()
                print(f"\nSleeping for {args.interval} seconds...")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nContinuous mode stopped by user.")
            sys.exit(0)
    else:
        run_pipeline()

if __name__ == "__main__":
    main()
