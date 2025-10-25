#!/usr/bin/env python3
"""
DuckDB Baseline Benchmark Demo
------------------------------

Reads data from a given folder (CSV or Parquet),
adds derived day/minute columns,
executes JSON queries, and reports timings.

Usage:
  python main.py --data-dir ./data --out-dir ./out
"""

import duckdb
import time
from pathlib import Path
import csv
import argparse
from assembler import assemble_sql
from inputs import queries
# from judges import queries


# -------------------
# Configuration
# -------------------
DB_PATH = Path("tmp/baseline.duckdb")
TABLE_NAME = "events"


# -------------------
# Convert CSV to Parquet
# -------------------
def convert_csv_to_parquet(con, data_dir: Path):
    """Convert CSV files to Parquet format using DuckDB for faster loading."""
    csv_files = sorted(data_dir.glob("events_part_*.csv"))
    parquet_files = list(data_dir.glob("events_part_*.parquet"))
    
    if parquet_files:
        print(f"🟩 Found {len(parquet_files)} existing Parquet files")
        return
    
    if not csv_files:
        raise FileNotFoundError(f"No events_part_*.csv found in {data_dir}")
    
    print(f"🟩 Converting {len(csv_files)} CSV files to Parquet format using DuckDB...")
    
    for csv_file in csv_files:
        parquet_file = csv_file.with_suffix('.parquet')
        print(f"  Converting {csv_file.name}...")
        
        # Use DuckDB to read CSV and write Parquet
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_csv(
                    '{csv_file}',
                    AUTO_DETECT = FALSE,
                    HEADER = TRUE,
                    COLUMNS = {{
                        'ts': 'VARCHAR',
                        'type': 'VARCHAR',
                        'auction_id': 'VARCHAR',
                        'advertiser_id': 'VARCHAR',
                        'publisher_id': 'VARCHAR',
                        'bid_price': 'VARCHAR',
                        'user_id': 'VARCHAR',
                        'total_price': 'VARCHAR',
                        'country': 'VARCHAR'
                    }}
                )
            )
            TO '{parquet_file}' (FORMAT PARQUET);
        """)
    
    print(f"🟩 Conversion complete")


# -------------------
# Load Data
# -------------------
def load_data(con, data_dir: Path):
    # First, convert CSV to Parquet if needed
    convert_csv_to_parquet(con, data_dir)
    
    parquet_files = sorted(data_dir.glob("events_part_*.parquet"))
    
    if parquet_files:
        print(f"🟩 Loading {len(parquet_files)} Parquet parts from {data_dir} ...")
        con.execute(f"""
            CREATE OR REPLACE VIEW {TABLE_NAME} AS
            WITH raw AS (
              SELECT *
              FROM read_parquet(
                '{data_dir}/events_part_*.parquet'
              )
            ),
            casted AS (
              SELECT
                to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                type,
                auction_id,
                TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                TRY_CAST(user_id AS BIGINT)               AS user_id,
                NULLIF(total_price, '')::DOUBLE           AS total_price,
                country
              FROM raw
            )
            SELECT
              ts,
              DATE_TRUNC('week', ts)              AS week,
              DATE(ts)                            AS day,
              DATE_TRUNC('hour', ts)              AS hour,
              STRFTIME(ts, '%Y-%m-%d %H:%M')      AS minute,
              type,
              auction_id,
              advertiser_id,
              publisher_id,
              bid_price,
              user_id,
              total_price,
              country
            FROM casted;
        """)
        print(f"🟩 Loading complete")
    else:
        raise FileNotFoundError(f"No events_part_*.parquet found in {data_dir}")


# -------------------
# Run Queries
# -------------------
def run(queries, data_dir: Path, out_dir: Path):
    # Ensure directories exist
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    load_data(con, data_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for i, q in enumerate(queries, 1):
        sql = assemble_sql(q)
        print(f"\n🟦 Query {i}:\n{q}\n")
        t0 = time.time()
        res = con.execute(sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        dt = time.time() - t0

        print(f"✅ Rows: {len(rows)} | Time: {dt:.3f}s")

        out_path = out_dir / f"q{i}.csv"
        with out_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)

        results.append({"query": i, "rows": len(rows), "time": dt})
    con.close()

    print("\nSummary:")
    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")
    print(f"Total time: {sum(r['time'] for r in results):.3f}s")


# -------------------
# Main Entry Point
# -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DuckDB Baseline Benchmark Demo — runs benchmark queries on input CSV data."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="The folder where the input CSV is provided"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Where to output query results-full"
    )

    args = parser.parse_args()
    run(queries, args.data_dir, args.out_dir)
