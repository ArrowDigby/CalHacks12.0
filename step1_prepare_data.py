#!/usr/bin/env python3
"""
Step 1: Prepare Data
--------------------
Reads CSV files and creates:
1. Persisted table (events_persisted) in DuckDB
2. View (events_raw) pointing to the persisted table for fallback queries

Usage:
  python step1_prepare_data.py --data-dir ./data
"""

import duckdb
import time
import argparse
from pathlib import Path
import os

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")
TABLE_NAME = "events"
PERSISTED_TABLE = "events_persisted"


def load_data(con, data_dir: Path):
    """Load CSV data into a view with proper typing and derived columns."""
    csv_files = list(data_dir.glob("events_part_*.csv"))

    if csv_files:
        print(f"🟩 Loading {len(csv_files)} CSV parts from {data_dir} ...")
        con.execute(f"""
            CREATE OR REPLACE VIEW {TABLE_NAME} AS
            WITH raw AS (
              SELECT *
              FROM read_csv(
                '{data_dir}/events_part_*.csv',
                AUTO_DETECT = FALSE,
                HEADER = TRUE,
                union_by_name = TRUE,
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
        raise FileNotFoundError(f"No events_part_*.csv found in {data_dir}")


def create_persisted_table(con):
    """Create a physical table from the events view."""
    print("🟩 Creating persisted table from events view...")
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE {PERSISTED_TABLE} AS SELECT * FROM {TABLE_NAME};")
    print(f"   ✓ Persisted table created in {time.time() - t0:.1f}s")


def create_fallback_view(con):
    """Create a view that points to the persisted table for queries that don't use rollups."""
    print("🟩 Creating fallback view (events_raw → events_persisted)...")
    con.execute(f"""
        CREATE OR REPLACE VIEW events_raw AS
        SELECT * FROM {PERSISTED_TABLE};
    """)
    print("   ✓ View created")


def main():
    parser = argparse.ArgumentParser(
        description="Step 1: Prepare data - create persisted table in DuckDB"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="The folder where the input CSV files are located"
    )

    args = parser.parse_args()

    # Ensure database directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    
    # Tune DuckDB for optimal performance
    threads = os.cpu_count() or 4
    con.execute(f"PRAGMA threads={threads};")
    mem_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "14GB")
    con.execute(f"PRAGMA memory_limit='{mem_limit}';")
    con.execute("SET preserve_insertion_order=false;")

    print("=" * 60)
    print("STEP 1: PREPARE DATA")
    print("=" * 60)
    print(f"📊 Using {threads} threads and {mem_limit} memory limit")

    # Load and prepare data
    load_data(con, args.data_dir)
    create_persisted_table(con)
    create_fallback_view(con)

    con.close()

    print("\n✅ Step 1 complete! Data is prepared and ready.")
    print(f"   Next: Run step2_build_rollups.py")


if __name__ == "__main__":
    main()

