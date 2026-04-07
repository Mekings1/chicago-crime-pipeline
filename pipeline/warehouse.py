import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET   = os.getenv("S3_BUCKET_NAME")
AWS_KEY     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET  = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION  = os.getenv("AWS_REGION")
DB_PATH     = "database/chicago_crime.duckdb"

S3_PATH = f"s3://{S3_BUCKET}/raw/chicago_crime/year=*/month=*/data.parquet"

def get_connection():
    os.makedirs("database", exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"""
        SET s3_region     = '{AWS_REGION}';
        SET s3_access_key_id     = '{AWS_KEY}';
        SET s3_secret_access_key = '{AWS_SECRET}';
    """)
    return con

def create_raw_table(con):
    print("Creating raw table from S3 Parquet files...")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_chicago_crime AS
        SELECT
            id,
            case_number,
            CAST(date AS TIMESTAMP)     AS incident_datetime,
            CAST(date AS DATE)          AS incident_date,
            EXTRACT(year  FROM CAST(date AS DATE))::INTEGER AS year,
            EXTRACT(month FROM CAST(date AS DATE))::INTEGER AS month,
            block,
            primary_type,
            description,
            location_description,
            arrest,
            domestic,
            district,
            ward,
            community_area,
            latitude,
            longitude
        FROM read_parquet('{S3_PATH}', hive_partitioning=true)
        WHERE date IS NOT NULL
          AND primary_type IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_chicago_crime").fetchone()[0]
    print(f"raw_chicago_crime created: {count:,} rows")

def create_indexes(con):
    print("Creating indexes for upstream queries...")
    # Partition-style sort on year+month (how DuckDB clusters data)
    con.execute("""
        CREATE OR REPLACE TABLE raw_chicago_crime AS
        SELECT * FROM raw_chicago_crime
        ORDER BY year, month, primary_type
    """)
    print("Table sorted by year, month, primary_type")

def show_summary(con):
    print("\n--- Sample ---")
    print(con.execute("SELECT primary_type, COUNT(*) as cnt FROM raw_chicago_crime GROUP BY 1 ORDER BY 2 DESC LIMIT 5").df())

if __name__ == "__main__":
    con = get_connection()
    create_raw_table(con)
    create_indexes(con)
    show_summary(con)
    con.close()
    print("\nDuckDB warehouse ready.")