import os
import io
import json
import time
import argparse
import boto3
import pandas as pd
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger

load_dotenv()

S3_BUCKET      = os.getenv("S3_BUCKET_NAME")
AWS_REGION     = os.getenv("AWS_REGION")
SOCRATA_URL    = os.getenv("SOCRATA_BASE_URL")
PAGE_SIZE      = int(os.getenv("PAGE_SIZE", 10000))
MANIFEST_KEY   = "raw/chicago_crime/manifest.json"

# ── S3 helpers ────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

def load_manifest(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception:
        return {}

def save_manifest(s3, manifest: dict):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json"
    )

# ── Core tasks ────────────────────────────────────────────────────────────────

@task(name="fetch-month", retries=3, retry_delay_seconds=10)
def fetch_month(year: int, month: int) -> pd.DataFrame:
    logger = get_run_logger()
    start = date(year, month, 1)
    end   = start + relativedelta(months=1)

    start_str = start.strftime("%Y-%m-%dT00:00:00.000")
    end_str   = end.strftime("%Y-%m-%dT00:00:00.000")

    logger.info(f"Fetching {year}-{month:02d} from Socrata...")

    pages, offset = [], 0
    while True:
        params = {
            "$where":  f"date >= '{start_str}' AND date < '{end_str}'",
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            "$order":  "date ASC",
        }
        resp = requests.get(SOCRATA_URL, params=params, timeout=60)
        resp.raise_for_status()

        chunk = pd.read_csv(io.BytesIO(resp.content), low_memory=False)
        if chunk.empty:
            break

        pages.append(chunk)
        logger.info(f"  Page offset={offset}: {len(chunk)} rows")

        if len(chunk) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(4.5)   # be polite to Socrata

    if not pages:
        logger.warning(f"No data found for {year}-{month:02d}")
        return pd.DataFrame()

    df = pd.concat(pages, ignore_index=True)
    logger.info(f"Total rows for {year}-{month:02d}: {len(df):,}")
    return df


@task(name="upload-to-s3")
def upload_to_s3(df: pd.DataFrame, year: int, month: int) -> str:
    logger = get_run_logger()
    s3_key = f"raw/chicago_crime/year={year}/month={month:02d}/data.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = get_s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    logger.info(f"Uploaded → s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name="chicago-crime-ingestion")
def ingest_pipeline(start_ym: str, end_ym: str):
    logger = get_run_logger()
    s3       = get_s3_client()
    manifest = load_manifest(s3)

    # Build list of (year, month) tuples in range
    start_dt = datetime.strptime(start_ym, "%Y-%m")
    end_dt   = datetime.strptime(end_ym,   "%Y-%m")

    periods = []
    cur = start_dt
    while cur <= end_dt:
        periods.append((cur.year, cur.month))
        cur += relativedelta(months=1)

    logger.info(f"Range: {start_ym} → {end_ym} | {len(periods)} months")

    for year, month in periods:
        key = f"{year}-{month:02d}"

        if manifest.get(key) == "done":
            logger.info(f"Skipping {key} (already in manifest)")
            continue

        df = fetch_month(year, month)

        if df.empty:
            manifest[key] = "empty"
            save_manifest(s3, manifest)
            continue

        upload_to_s3(df, year, month)
        manifest[key] = "done"
        save_manifest(s3, manifest)
        logger.info(f"Manifest updated: {key} = done")

    logger.info("Ingestion complete.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start month YYYY-MM")
    parser.add_argument("--end",   required=True, help="End month   YYYY-MM")
    args = parser.parse_args()
    ingest_pipeline(start_ym=args.start, end_ym=args.end)