import os
import subprocess
import sys
import argparse
import duckdb
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger

from ingest import ingest_pipeline
from warehouse import get_connection, create_raw_table, create_indexes, show_summary

load_dotenv()

DB_PATH = "database/chicago_crime.duckdb"

# ── Task 1: Ingest ─────────────────────────────────────────────────────────────

@task(name="run-ingestion")
def run_ingestion(start_ym: str, end_ym: str):
    logger = get_run_logger()
    logger.info(f"Starting ingestion: {start_ym} → {end_ym}")
    ingest_pipeline(start_ym=start_ym, end_ym=end_ym)
    logger.info("Ingestion complete.")

# ── Task 2: Build warehouse ────────────────────────────────────────────────────

@task(name="build-warehouse")
def build_warehouse():
    logger = get_run_logger()
    logger.info("Building DuckDB warehouse...")
    con = get_connection()
    create_raw_table(con)
    create_indexes(con)
    show_summary(con)
    con.close()
    logger.info("Warehouse ready.")

# ── Task 3: Run dbt ────────────────────────────────────────────────────────────

@task(name="run-dbt")
def run_dbt():
    logger = get_run_logger()
    dbt_dir = os.path.join("dbt_project", "chicago_crime")

    logger.info("Running dbt models...")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=dbt_dir,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run failed — check logs above.")

    logger.info("Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test"],
        cwd=dbt_dir,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt test failed — check logs above.")

    logger.info("dbt models and tests passed.")

# ── Task 4: Launch dashboard ───────────────────────────────────────────────────

@task(name="launch-dashboard")
def launch_dashboard():
    logger = get_run_logger()
    logger.info("Launching Streamlit dashboard at http://localhost:8501")
    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "dashboard/app.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    logger.info("Dashboard is running. Open http://localhost:8501 in your browser.")

# ── Master flow ────────────────────────────────────────────────────────────────

@flow(name="chicago-crime-master-pipeline")
def master_pipeline(start_ym: str, end_ym: str):
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info("  Chicago Crime Pipeline — Full Run")
    logger.info("=" * 50)

    run_ingestion(start_ym=start_ym, end_ym=end_ym)
    build_warehouse()
    run_dbt()
    launch_dashboard()

    logger.info("=" * 50)
    logger.info("  Pipeline complete. Dashboard live.")
    logger.info("=" * 50)

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chicago Crime Master Pipeline")
    parser.add_argument("--start", required=True, help="Start month YYYY-MM")
    parser.add_argument("--end",   required=True, help="End month   YYYY-MM")
    args = parser.parse_args()

    master_pipeline(start_ym=args.start, end_ym=args.end)