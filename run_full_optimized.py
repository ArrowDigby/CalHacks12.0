#!/usr/bin/env python3
"""
Full Optimized Pipeline for 7-Minute Prep Time
----------------------------------------------
Runs all optimization steps in the optimal sequence for maximum performance.

Strategy:
1. Smart Z-ordered Parquet (impression events only)
2. Enhanced rollups with indexes
3. Optimized query execution

Usage:
  python run_full_optimized.py --data-dir ./data --out-dir ./output
"""

import subprocess
import sys
import time
from pathlib import Path

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run full optimized pipeline for maximum performance"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing CSV files"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for results"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("FULL OPTIMIZED PIPELINE - 7 MINUTE PREP")
    print("=" * 70)
    print()
    print("Strategy:")
    print("  ✅ Smart Z-ordering (impression events)")
    print("  ✅ Enhanced rollups with indexes")
    print("  ✅ Optimized query routing")
    print("  ✅ Advanced caching")
    print()
    
    total_start = time.time()
    
    # Step 1: Prepare data with smart Z-ordering
    print("🔹 STEP 1: Prepare Data with Smart Z-ordering")
    print("-" * 70)
    step1_start = time.time()
    
    result = subprocess.run([
        sys.executable, "step1_prepare_data.py",
        "--data-dir", str(args.data_dir),
        "--out-dir", str(args.out_dir),
        "--smart-zorder"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    
    if result.returncode != 0:
        print(f"❌ Step 1 failed with return code {result.returncode}")
        return
    
    step1_time = time.time() - step1_start
    print(f"✅ Step 1 completed in {step1_time:.1f}s")
    print()
    
    # Step 2: Build optimized rollups
    print("🔹 STEP 2: Build Optimized Rollups")
    print("-" * 70)
    step2_start = time.time()
    
    result = subprocess.run([
        sys.executable, "step2_build_rollups_optimized.py"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    
    if result.returncode != 0:
        print(f"❌ Step 2 failed with return code {result.returncode}")
        return
    
    step2_time = time.time() - step2_start
    print(f"✅ Step 2 completed in {step2_time:.1f}s")
    print()
    
    total_time = time.time() - total_start
    
    print("=" * 70)
    print("✅ FULL OPTIMIZED PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Prep time: {total_time:.1f}s of 420s ({total_time/420*100:.1f}%)")
    print(f"Remaining buffer: {420 - total_time:.1f}s")
    print()
    print("Optimizations applied:")
    print("  ✅ Smart Z-ordering for impression events")
    print("  ✅ Multi-dimensional partitioning")
    print("  ✅ Strategic indexes on all rollups")
    print("  ✅ Enhanced rollup coverage (12 rollups)")
    print()
    print("Next steps:")
    print(f"  python step3_run_queries_optimized.py --out-dir {args.out_dir}")
    print()
    print("Expected performance:")
    print("  - Rollup queries: 0.1-1s per query")
    print("  - Edge cases (impressions): 3-10s (with Z-order)")
    print("  - Edge cases (others): 10-30s (acceptable)")
    print()

if __name__ == "__main__":
    main()

