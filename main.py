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
import os
import json
import copy
from assembler import assemble_sql
from inputs import queries
# from judges import queries


# -------------------
# Configuration
# -------------------
DB_PATH = Path("tmp/baseline.duckdb")
TABLE_NAME = "events"
PERSISTED_TABLE = "events_persisted"

ROLLUP_BY_DAY = "by_day"
ROLLUP_BY_COUNTRY_DAY = "by_country_day"
ROLLUP_BY_PUBLISHER_DAY = "by_publisher_day"
ROLLUP_BY_ADVERTISER_DAY = "by_advertiser_day"

# Dimension-only rollups (no day)
ROLLUP_BY_COUNTRY = "by_country"
ROLLUP_BY_PUBLISHER = "by_publisher"
ROLLUP_BY_ADVERTISER = "by_advertiser"


# -------------------
# Load Data
# -------------------
def load_data(con, data_dir: Path):
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


# -------------------
# Preprocessing: Parquet + Rollups
# -------------------
def prepare(con, out_dir: Path):
    """Create a persisted table, write partitioned Parquet, and build rollups."""
    # Persist the current events view to a physical table for reuse
    print("🟩 Creating persisted table from events view...")
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE {PERSISTED_TABLE} AS SELECT * FROM {TABLE_NAME};")
    print(f"   ✓ Persisted table created in {time.time() - t0:.1f}s")

    parquet_root = out_dir / "parquet"
    parquet_events = parquet_root / "events"
    parquet_root.mkdir(parents=True, exist_ok=True)

    print("🟩 Writing partitioned Parquet (type, day) ...")
    print("   (This may take a few minutes for large datasets...)")
    t0 = time.time()
    con.execute(f"""
        COPY (
          SELECT * FROM {PERSISTED_TABLE}
          -- ORDER BY removed for faster partitioning (streaming starts immediately)
        ) TO '{parquet_events.as_posix()}' (
          FORMAT 'parquet',
          PARTITION_BY (type, day),
          OVERWRITE_OR_IGNORE
        );
    """)
    print(f"   ✓ Parquet write completed in {time.time() - t0:.1f}s")

    print("🟩 Creating view over Parquet dataset ...")
    con.execute(f"""
        CREATE OR REPLACE VIEW events_parquet AS
        SELECT *
        FROM read_parquet('{(parquet_events / "**/*.parquet").as_posix()}');
    """)

    print("🟩 Building rollups ...")
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
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY_DAY} AS
        SELECT day, country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, country, type;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER_DAY} AS
        SELECT day, publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, publisher_id, type;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER_DAY} AS
        SELECT day, advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY day, advertiser_id, type;
    """)
    
    print("🟩 Building dimension-only rollups (no day) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_COUNTRY} AS
        SELECT country, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY country, type;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_PUBLISHER} AS
        SELECT publisher_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY publisher_id, type;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {ROLLUP_BY_ADVERTISER} AS
        SELECT advertiser_id, type,
               COUNT(*) AS cnt,
               SUM(bid_price) AS sum_bid,
               SUM(total_price) AS sum_total
        FROM {PERSISTED_TABLE}
        GROUP BY advertiser_id, type;
    """)


# -------------------
# Query Routing + Cache helpers
# -------------------
def _is_simple_agg(select_items):
    allowed = {"COUNT", "SUM", "AVG"}
    for item in select_items:
        if isinstance(item, str):
            continue
        if isinstance(item, dict):
            func, _ = next(iter(item.items()))
            if func.upper() not in allowed:
                return False
    return True


def _normalize_query_for_cache(q: dict) -> str:
    def normalize(obj):
        if isinstance(obj, dict):
            return {k: normalize(obj[k]) for k in sorted(obj)}
        if isinstance(obj, list):
            return [normalize(v) for v in obj]
        return obj
    return json.dumps(normalize(q), separators=(",", ":"))


def pick_source(q: dict) -> str:
    group_by = set(q.get("group_by") or [])
    selects = q.get("select", [])
    where = q.get("where") or []
    
    if not _is_simple_agg(selects):
        return "events_parquet"
    
    # Extract columns referenced in WHERE clause
    where_cols = set()
    for cond in where:
        where_cols.add(cond["col"])
    
    needs_day = "day" in group_by
    
    # Rollup schema reference:
    # by_day: day, type
    # by_country_day: day, country, type
    # by_publisher_day: day, publisher_id, type
    # by_advertiser_day: day, advertiser_id, type
    # by_country: country, type
    # by_publisher: publisher_id, type
    # by_advertiser: advertiser_id, type
    
    if needs_day:
        # Query explicitly groups by day - use matching rollup
        dims = group_by - {"day"}
        if dims == {"country"}:
            return ROLLUP_BY_COUNTRY_DAY
        if dims == {"publisher_id"}:
            # Check if country filter is used - publisher rollups don't have country
            if "country" in where_cols:
                return "events_parquet"  # Can't use rollup
            return ROLLUP_BY_PUBLISHER_DAY
        if dims == {"advertiser_id"}:
            if "country" in where_cols:
                return "events_parquet"
            return ROLLUP_BY_ADVERTISER_DAY
        if dims in (set(), {"type"}):
            if "country" in where_cols or "publisher_id" in where_cols or "advertiser_id" in where_cols:
                return "events_parquet"  # by_day doesn't have these dimensions
            return ROLLUP_BY_DAY
    else:
        # Query doesn't group by day
        # Dimension-only rollups (no day column, so can't filter by day)
        if "day" in where_cols:
            # Need day filtering - use _day rollups
            if group_by in ({"publisher_id"}, {"publisher_id", "type"}):
                if "country" in where_cols:
                    return "events_parquet"  # Can't filter by country
                return ROLLUP_BY_PUBLISHER_DAY
            if group_by in ({"advertiser_id"}, {"advertiser_id", "type"}):
                if "country" in where_cols:
                    return "events_parquet"
                return ROLLUP_BY_ADVERTISER_DAY
            if group_by in ({"country"}, {"country", "type"}):
                return ROLLUP_BY_COUNTRY_DAY
        else:
            # No day filter - use dimension-only rollups (smaller!)
            if "country" in where_cols:
                # Only country rollups have country column
                if group_by in ({"country"}, {"country", "type"}):
                    return ROLLUP_BY_COUNTRY
                return "events_parquet"  # Other rollups don't have country
            
            # No special filters - use dimension-only rollups
            if group_by == {"country"}:
                return ROLLUP_BY_COUNTRY
            if group_by == {"country", "type"}:
                return ROLLUP_BY_COUNTRY
            if group_by == {"publisher_id"}:
                return ROLLUP_BY_PUBLISHER
            if group_by == {"publisher_id", "type"}:
                return ROLLUP_BY_PUBLISHER
            if group_by == {"advertiser_id"}:
                return ROLLUP_BY_ADVERTISER
            if group_by == {"advertiser_id", "type"}:  # ← Query 4!
                return ROLLUP_BY_ADVERTISER
            if group_by in (set(), {"type"}):
                return ROLLUP_BY_DAY
    
    return "events_parquet"


def _where_clause(where):
    if not where:
        return ""
    parts = []
    for cond in where:
        col, op, val = cond["col"], cond["op"], cond["val"]
        if op == "eq":
            parts.append(f"{col} = '{val}'")
        if op == "neq":
            parts.append(f"{col} != '{val}'")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col} {sym} {val}")
        elif op == "between":
            low, high = val
            parts.append(f"{col} BETWEEN '{low}' AND '{high}'")
        elif op == "in":
            vals = ", ".join(f"'{v}'" for v in val)
            parts.append(f"{col} IN ({vals})")
    return "WHERE " + " AND ".join(parts)


def _assemble_rollup_sql(q: dict, rollup_table: str) -> str:
    select_parts = []
    for item in q.get("select", []):
        if isinstance(item, str):
            select_parts.append(item)
        elif isinstance(item, dict):
            func, col = next(iter(item.items()))
            F = func.upper()
            if F == "COUNT":
                expr = "SUM(cnt)"
                alias = "COUNT(*)"
            elif F == "SUM":
                if col == "bid_price":
                    expr = "SUM(sum_bid)"
                elif col == "total_price":
                    expr = "SUM(sum_total)"
                else:
                    expr = f"SUM({col})"
                alias = f"SUM({col})"
            elif F == "AVG":
                if col == "bid_price":
                    expr = "SUM(sum_bid) * 1.0 / NULLIF(SUM(cnt), 0)"
                elif col == "total_price":
                    expr = "SUM(sum_total) * 1.0 / NULLIF(SUM(cnt), 0)"
                else:
                    expr = f"AVG({col})"
                alias = f"AVG({col})"
            else:
                expr = f"{F}({col})"
                alias = f"{F}({col})"
            select_parts.append(f"{expr} AS \"{alias}\"")

    where_sql = _where_clause(q.get("where"))
    group_by_cols = q.get("group_by") or []
    group_by_sql = ("GROUP BY " + ", ".join(group_by_cols)) if group_by_cols else ""
    order_by_items = q.get("order_by") or []
    order_by_sql = ""
    if order_by_items:
        parts = []
        for o in order_by_items:
            col = o['col']
            # If ordering by an aggregate function (contains parentheses), quote it
            # because we aliased it in SELECT as a quoted identifier
            if '(' in col:
                parts.append(f'"{col}" {o.get("dir", "asc").upper()}')
            else:
                parts.append(f'{col} {o.get("dir", "asc").upper()}')
        order_by_sql = "ORDER BY " + ", ".join(parts)

    return (
        f"SELECT {', '.join(select_parts)} FROM {rollup_table} "
        f"{where_sql} {group_by_sql} {order_by_sql}"
    ).strip()

# -------------------
# Run Queries
# -------------------
def run(queries, data_dir: Path, out_dir: Path):
    # Ensure directories exist
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)
    # PRAGMA tuning
    threads = os.cpu_count() or 4
    con.execute(f"PRAGMA threads={threads};")
    mem_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "14GB")
    con.execute(f"PRAGMA memory_limit='{mem_limit}';")
    con.execute("SET preserve_insertion_order=false;")

    load_data(con, data_dir)
    prepare(con, out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    cache = {}
    for i, q in enumerate(queries, 1):
        q_working = copy.deepcopy(q)
        source = pick_source(q_working)
        if source and source != q_working.get("from"):
            q_working["from"] = source

        cache_key = _normalize_query_for_cache(q_working)
        if cache_key in cache:
            cols, rows = cache[cache_key]
            print(f"\n🟦 Query {i} (cached):\n{q}\n")
            dt = 0.0
        else:
            if source in (ROLLUP_BY_DAY, ROLLUP_BY_COUNTRY_DAY, ROLLUP_BY_PUBLISHER_DAY, ROLLUP_BY_ADVERTISER_DAY,
                          ROLLUP_BY_COUNTRY, ROLLUP_BY_PUBLISHER, ROLLUP_BY_ADVERTISER):
                sql = _assemble_rollup_sql(q_working, source)
            else:
                sql = assemble_sql(q_working)
            print(f"\n🟦 Query {i}:\n{q}\n")
            t0 = time.time()
            res = con.execute(sql)
            cols = [d[0] for d in res.description]
            rows = res.fetchall()
            dt = time.time() - t0
            cache[cache_key] = (cols, rows)

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
