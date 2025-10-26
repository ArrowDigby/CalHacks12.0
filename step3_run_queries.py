#!/usr/bin/env python3
"""
Step 3: Run Queries
-------------------
Executes benchmark queries using optimized routing and caching.

Features:
- Automatic rollup routing for fast queries
- Result caching for repeated queries
- Query performance tracking
- Terminal output only (no CSV files)

Usage:
  python step3_run_queries.py
"""

import duckdb
import time
from pathlib import Path
import argparse
import json
import copy
import csv
import pandas as pd
from assembler import assemble_sql
from inputs import queries

# Configuration
DB_PATH = Path("tmp/baseline.duckdb")

# Rollup table names
ROLLUP_BY_DAY = "by_day"
ROLLUP_BY_COUNTRY_DAY = "by_country_day"
ROLLUP_BY_PUBLISHER_DAY = "by_publisher_day"
ROLLUP_BY_ADVERTISER_DAY = "by_advertiser_day"
ROLLUP_BY_PUBLISHER_COUNTRY_DAY = "by_publisher_country_day"
ROLLUP_BY_MINUTE = "by_minute"
ROLLUP_BY_COUNTRY = "by_country"
ROLLUP_BY_PUBLISHER = "by_publisher"
ROLLUP_BY_ADVERTISER = "by_advertiser"


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
    
    # If no GROUP BY, this is a raw data query (filtering/selecting, not aggregating)
    if not group_by:
        return "events_parquet"
    
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
    # by_publisher_country_day: day, publisher_id, country, type
    # by_minute: minute, day, type
    # by_country: country, type
    # by_publisher: publisher_id, type
    # by_advertiser: advertiser_id, type
    
    # Check for minute-level queries first
    if "minute" in group_by:
        # Minute-level queries can use by_minute rollup if only grouping by minute + type
        if group_by in ({"minute"}, {"minute", "type"}):
            # Check if filters are compatible (only type and day are in by_minute)
            if where_cols - {"type", "day"} == set():
                # Can filter by type and day, but need to apply day filter in WHERE
                return ROLLUP_BY_MINUTE
        return "events_parquet"  # Need full data for complex minute queries
    
    if needs_day:
        # Query explicitly groups by day - use matching rollup
        dims = group_by - {"day"}
        if dims == {"country"}:
            return ROLLUP_BY_COUNTRY_DAY
        if dims == {"publisher_id"}:
            # Check if country filter is used
            if "country" in where_cols:
                # Use the new 3-dimensional rollup
                return ROLLUP_BY_PUBLISHER_COUNTRY_DAY
            return ROLLUP_BY_PUBLISHER_DAY
        if dims == {"advertiser_id"}:
            if "country" in where_cols:
                return "events_parquet"  # No advertiser+country+day rollup
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
                    # Use the new 3-dimensional rollup
                    return ROLLUP_BY_PUBLISHER_COUNTRY_DAY
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
            if group_by == {"advertiser_id", "type"}:
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


def compare_with_truth(result_csv: Path, truth_csv: Path) -> dict:
    """Compare result CSV with ground truth CSV."""
    if not truth_csv.exists():
        return {"status": "SKIP", "message": "Truth file not found", "details": None}
    
    try:
        # Read both CSVs
        result_df = pd.read_csv(result_csv)
        truth_df = pd.read_csv(truth_csv)
        
        # Check row counts
        if len(result_df) != len(truth_df):
            return {
                "status": "FAIL",
                "message": f"Row count mismatch: got {len(result_df)}, expected {len(truth_df)}",
                "details": None
            }
        
        # Check column counts
        if len(result_df.columns) != len(truth_df.columns):
            return {
                "status": "FAIL",
                "message": f"Column count mismatch: got {len(result_df.columns)}, expected {len(truth_df.columns)}",
                "details": f"Result columns: {list(result_df.columns)}\nTruth columns: {list(truth_df.columns)}"
            }
        
        # Check if column names match
        if set(result_df.columns) != set(truth_df.columns):
            return {
                "status": "FAIL",
                "message": "Column names don't match",
                "details": f"Result columns: {list(result_df.columns)}\nTruth columns: {list(truth_df.columns)}"
            }
        
        # Sort both dataframes by all columns for consistent comparison
        result_sorted = result_df.sort_values(by=list(result_df.columns)).reset_index(drop=True)
        truth_sorted = truth_df.sort_values(by=list(truth_df.columns)).reset_index(drop=True)
        
        # Compare values (with tolerance for floating point)
        try:
            pd.testing.assert_frame_equal(
                result_sorted,
                truth_sorted,
                check_dtype=False,
                rtol=1e-5,
                atol=1e-8
            )
            return {"status": "PASS", "message": "Exact match", "details": None}
        except AssertionError as e:
            # Find rows with differences
            mismatched_rows = []
            max_rows_to_show = 5
            
            for idx in range(len(result_sorted)):
                row_has_diff = False
                diff_columns = []
                
                for col in result_sorted.columns:
                    result_val = result_sorted.iloc[idx][col]
                    truth_val = truth_sorted.iloc[idx][col]
                    
                    # Check if values are different
                    are_different = False
                    if pd.isna(result_val) and pd.isna(truth_val):
                        continue  # Both NaN, consider equal
                    elif pd.isna(result_val) or pd.isna(truth_val):
                        are_different = True  # One is NaN, other isn't
                    elif isinstance(result_val, (int, float)) and isinstance(truth_val, (int, float)):
                        # Numeric comparison with tolerance
                        if abs(result_val - truth_val) > 1e-8 + 1e-5 * abs(truth_val):
                            are_different = True
                    else:
                        # String comparison
                        if str(result_val) != str(truth_val):
                            are_different = True
                    
                    if are_different:
                        row_has_diff = True
                        diff_columns.append({
                            "column": col,
                            "result": result_val,
                            "truth": truth_val
                        })
                
                if row_has_diff:
                    mismatched_rows.append({
                        "row": idx,
                        "diff_columns": diff_columns,
                        "result_row": result_sorted.iloc[idx].to_dict(),
                        "truth_row": truth_sorted.iloc[idx].to_dict()
                    })
                    
                    if len(mismatched_rows) >= max_rows_to_show:
                        break
            
            # Format differences for display
            if mismatched_rows:
                details_lines = [f"Found {len(mismatched_rows)}+ mismatched rows (showing first {len(mismatched_rows)}):"]
                details_lines.append("")
                
                for mismatch in mismatched_rows:
                    details_lines.append(f"  Row {mismatch['row']}:")
                    
                    # Show which columns differ
                    diff_cols = [d['column'] for d in mismatch['diff_columns']]
                    details_lines.append(f"    Columns with differences: {', '.join(diff_cols)}")
                    
                    # Show complete result row
                    details_lines.append(f"    Result row:  {mismatch['result_row']}")
                    
                    # Show complete truth row
                    details_lines.append(f"    Truth row:   {mismatch['truth_row']}")
                    
                    # Show specific differences
                    details_lines.append(f"    Differences:")
                    for diff in mismatch['diff_columns']:
                        details_lines.append(
                            f"      - {diff['column']}: got {diff['result']!r}, expected {diff['truth']!r}"
                        )
                    
                    details_lines.append("")  # Blank line between rows
                
                details = "\n".join(details_lines)
            else:
                details = str(e)[:200]
            
            return {"status": "FAIL", "message": "Data mismatch", "details": details}
    
    except Exception as e:
        import traceback
        error_details = f"{str(e)}\n{traceback.format_exc()}"
        return {
            "status": "ERROR",
            "message": f"Comparison error: {str(e)[:100]}",
            "details": error_details[:500]
        }


def run_queries(con, queries_list, out_dir: Path, truth_dir: Path = None):
    """Execute queries with routing and caching."""
    results = []
    cache = {}
    
    for i, q in enumerate(queries_list, 1):
        q_working = copy.deepcopy(q)
        source = pick_source(q_working)
        if source and source != q_working.get("from"):
            q_working["from"] = source

        cache_key = _normalize_query_for_cache(q_working)
        if cache_key in cache:
            cols, rows = cache[cache_key]
            print(f"\n🟦 Query {i} (cached):")
            dt = 0.0
        else:
            if source in (ROLLUP_BY_DAY, ROLLUP_BY_COUNTRY_DAY, ROLLUP_BY_PUBLISHER_DAY, ROLLUP_BY_ADVERTISER_DAY,
                          ROLLUP_BY_PUBLISHER_COUNTRY_DAY, ROLLUP_BY_MINUTE,
                          ROLLUP_BY_COUNTRY, ROLLUP_BY_PUBLISHER, ROLLUP_BY_ADVERTISER):
                sql = _assemble_rollup_sql(q_working, source)
                print(f"\n🟦 Query {i} (using rollup: {source}):")
            else:
                sql = assemble_sql(q_working)
                print(f"\n🟦 Query {i} (using: {source}):")
            
            t0 = time.time()
            try:
                res = con.execute(sql)
                # Normalize column names to lowercase and replace count(*) with count_star()
                cols = [d[0].lower().replace('count(*)', 'count_star()') for d in res.description]
                rows = res.fetchall()
                dt = time.time() - t0
                cache[cache_key] = (cols, rows)
            except Exception as e:
                # Fallback to Parquet if rollup fails
                if source != "events_parquet":
                    print(f"   ⚠️  Rollup failed: {str(e)[:100]}")
                    print(f"   🔄 Falling back to Parquet files...")
                    
                    # Retry with Parquet
                    q_fallback = copy.deepcopy(q)
                    q_fallback["from"] = "events_parquet"
                    sql = assemble_sql(q_fallback)
                    
                    t0 = time.time()
                    res = con.execute(sql)
                    cols = [d[0].lower().replace('count(*)', 'count_star()') for d in res.description]
                    rows = res.fetchall()
                    dt = time.time() - t0
                    cache[cache_key] = (cols, rows)
                    source = "events_parquet (fallback)"
                else:
                    # Even Parquet failed - re-raise the error
                    raise

        print(f"   ✅ Rows: {len(rows):,} | Time: {dt:.3f}s | Source: {source}")

        # Write results to CSV
        csv_path = out_dir / f"q{i}.csv"
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"   💾 Saved to {csv_path}")

        # Compare with truth if provided
        comparison = None
        if truth_dir:
            truth_csv = truth_dir / f"q{i}.csv"
            comparison = compare_with_truth(csv_path, truth_csv)
            
            if comparison["status"] == "PASS":
                print(f"   ✅ PASS: {comparison['message']}")
            elif comparison["status"] == "FAIL":
                print(f"   ❌ FAIL: {comparison['message']}")
                if comparison.get('details'):
                    # Print details with indentation
                    for line in comparison['details'].split('\n'):
                        print(f"      {line}")
            elif comparison["status"] == "SKIP":
                print(f"   ⚠️  SKIP: {comparison['message']}")
            elif comparison["status"] == "ERROR":
                print(f"   ⚠️  ERROR: {comparison['message']}")
                if comparison.get('details'):
                    for line in comparison['details'].split('\n'):
                        print(f"      {line}")

        results.append({
            "query": i,
            "rows": len(rows),
            "time": dt,
            "source": source,
            "comparison": comparison
        })
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Step 3: Run queries with optimized routing and caching"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory to save query results (CSV files)"
    )
    parser.add_argument(
        "--truth-dir",
        type=Path,
        help="Directory with ground truth CSV files for comparison (optional)"
    )

    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"❌ Error: Database file not found at {DB_PATH}")
        print(f"   Please run step1_prepare_data.py and step2_build_rollups.py first!")
        return

    # Ensure output directory exists
    args.out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB_PATH)

    print("=" * 70)
    print("STEP 3: RUN QUERIES")
    print("=" * 70)
    print(f"📁 Output directory: {args.out_dir}")
    if args.truth_dir:
        print(f"🔍 Truth directory: {args.truth_dir}")

    results = run_queries(con, queries, args.out_dir, args.truth_dir)

    con.close()

    print("\n" + "=" * 70)
    print("PERFORMANCE SUMMARY")
    print("=" * 70)
    for r in results:
        source_type = "ROLLUP" if r['source'] in (ROLLUP_BY_DAY, ROLLUP_BY_COUNTRY_DAY, 
                                                    ROLLUP_BY_PUBLISHER_DAY, ROLLUP_BY_ADVERTISER_DAY,
                                                    ROLLUP_BY_PUBLISHER_COUNTRY_DAY, ROLLUP_BY_MINUTE,
                                                    ROLLUP_BY_COUNTRY, ROLLUP_BY_PUBLISHER, 
                                                    ROLLUP_BY_ADVERTISER) else "TABLE"
        status_str = ""
        if r.get('comparison'):
            comp = r['comparison']
            if comp['status'] == 'PASS':
                status_str = " | ✅ PASS"
            elif comp['status'] == 'FAIL':
                status_str = " | ❌ FAIL"
            elif comp['status'] == 'SKIP':
                status_str = " | ⚠️  SKIP"
            elif comp['status'] == 'ERROR':
                status_str = " | ⚠️  ERROR"
        
        print(f"Q{r['query']}: {r['time']:.3f}s | {r['rows']:,} rows | {source_type:6} | {r['source']}{status_str}")
    
    total_time = sum(r['time'] for r in results)
    print("-" * 70)
    print(f"Total time: {total_time:.3f}s")
    
    # Summary of test results if truth comparison was done
    if args.truth_dir:
        comparisons = [r.get('comparison') for r in results if r.get('comparison')]
        if comparisons:
            passed = sum(1 for c in comparisons if c['status'] == 'PASS')
            failed = sum(1 for c in comparisons if c['status'] == 'FAIL')
            skipped = sum(1 for c in comparisons if c['status'] == 'SKIP')
            errors = sum(1 for c in comparisons if c['status'] == 'ERROR')
            
            print("-" * 70)
            print(f"Test Results: {passed} PASS | {failed} FAIL | {skipped} SKIP | {errors} ERROR")
    
    print("=" * 70)
    
    print(f"\n✅ All queries complete! Results saved to {args.out_dir}")


if __name__ == "__main__":
    main()

