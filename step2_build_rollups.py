#!/usr/bin/env python3
"""
Step 2: Build Rollups
---------------------
Creates pre-aggregated rollup tables for fast query execution.

Creates rollups:
- by_day (day, type)
- by_country_day (day, country, type)
- by_publisher_day (day, publisher_id, type)
- by_advertiser_day (day, advertiser_id, type)
- by_publisher_country_day (day, publisher_id, country, type)
- by_advertiser_country_day (day, advertiser_id, country, type)
- by_publisher_advertiser_day (day, publisher_id, advertiser_id, type)
- by_minute (minute, day, type)
- by_country (country, type)
- by_publisher (publisher_id, type)
- by_advertiser (advertiser_id, type)

Usage:
  python step2_build_rollups.py
"""

import duckdb
import time
from pathlib import Path

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")
PERSISTED_TABLE = "events_persisted"

# Rollup table names
ROLLUP_BY_DAY = "by_day"
ROLLUP_BY_COUNTRY_DAY = "by_country_day"
ROLLUP_BY_PUBLISHER_DAY = "by_publisher_day"
ROLLUP_BY_ADVERTISER_DAY = "by_advertiser_day"
ROLLUP_BY_PUBLISHER_COUNTRY_DAY = "by_publisher_country_day"
ROLLUP_BY_ADVERTISER_COUNTRY_DAY = "by_advertiser_country_day"
ROLLUP_BY_PUBLISHER_ADVERTISER_DAY = "by_publisher_advertiser_day"
ROLLUP_BY_MINUTE = "by_minute"
ROLLUP_BY_COUNTRY = "by_country"
ROLLUP_BY_PUBLISHER = "by_publisher"
ROLLUP_BY_ADVERTISER = "by_advertiser"


def build_day_rollups(con):
    """Build rollups with day dimension."""
    print("🟩 Building day-level rollups...")
    
    print("   Building by_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_DAY} AS
        SELECT day, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, type;
    """)
    print(f"   ✓ by_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY_DAY} AS
        SELECT day, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, country, type;
    """)
    print(f"   ✓ by_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_DAY} AS
        SELECT day, publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, type;
    """)
    print(f"   ✓ by_publisher_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_DAY} AS
        SELECT day, advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, advertiser_id, type;
    """)
    print(f"   ✓ by_advertiser_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_COUNTRY_DAY} AS
        SELECT day, publisher_id, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, country, type;
    """)
    print(f"   ✓ by_publisher_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser_country_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_COUNTRY_DAY} AS
        SELECT day, advertiser_id, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, advertiser_id, country, type;
    """)
    print(f"   ✓ by_advertiser_country_day created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher_advertiser_day...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_ADVERTISER_DAY} AS
        SELECT day, publisher_id, advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, advertiser_id, type;
    """)
    print(f"   ✓ by_publisher_advertiser_day created in {time.time() - t0:.1f}s")


def build_minute_rollups(con):
    """Build minute-level rollups for fine-grained time analysis."""
    print("🟩 Building minute-level rollups...")
    
    print("   Building by_minute...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_MINUTE} AS
        SELECT minute, day, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY minute, day, type;
    """)
    print(f"   ✓ by_minute created in {time.time() - t0:.1f}s")


def build_dimension_rollups(con):
    """Build dimension-only rollups (no day)."""
    print("🟩 Building dimension-only rollups (no day) ...")
    
    print("   Building by_country...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY} AS
        SELECT country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY country, type;
    """)
    print(f"   ✓ by_country created in {time.time() - t0:.1f}s")
    
    print("   Building by_publisher...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER} AS
        SELECT publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY publisher_id, type;
    """)
    print(f"   ✓ by_publisher created in {time.time() - t0:.1f}s")
    
    print("   Building by_advertiser...")
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER} AS
        SELECT advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY advertiser_id, type;
    """)
    print(f"   ✓ by_advertiser created in {time.time() - t0:.1f}s")


def main():
    if not DB_PATH.exists():
        print(f"❌ Error: Database file not found at {DB_PATH}")
        print(f"   Please run step1_prepare_data.py first!")
        return

    con = duckdb.connect(DB_PATH)

    print("=" * 60)
    print("STEP 2: BUILD ROLLUPS")
    print("=" * 60)

    total_start = time.time()
    
    build_day_rollups(con)
    build_minute_rollups(con)
    build_dimension_rollups(con)
    
    total_time = time.time() - total_start

    con.close()

    print(f"\n✅ Step 2 complete! All rollups created in {total_time:.1f}s")
    print(f"   Next: Run step3_run_queries.py")


if __name__ == "__main__":
    main()

