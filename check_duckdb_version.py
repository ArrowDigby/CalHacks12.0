#!/usr/bin/env python3
"""
Quick DuckDB Version Check
Verifies you have DuckDB 1.2.0+ for bloom filter support
"""

import sys

try:
    import duckdb
    
    version = duckdb.__version__
    print(f"DuckDB version: {version}")
    
    # Parse version
    version_parts = version.split('.')
    major = int(version_parts[0])
    minor = int(version_parts[1])
    
    if (major, minor) >= (1, 2):
        print("✅ Version is 1.2.0 or higher - bloom filters supported!")
        print("\nYou're ready to generate Parquet files with bloom filters.")
        print("\nNext steps:")
        print("  1. Run: python convert_csv_to_parquet.py --data-dir /Users/matthewhu/Downloads/data --out-dir ./output")
        print("  2. Verify: python verify_bloom_filters.py --parquet-dir ./output/events")
        sys.exit(0)
    else:
        print("❌ Version is below 1.2.0 - bloom filters NOT supported!")
        print(f"\nCurrent version: {version}")
        print("Required version: 1.2.0 or higher")
        print("\nTo upgrade, run:")
        print("  pip install --upgrade 'duckdb>=1.2.0'")
        print("\nThen run this script again to verify.")
        sys.exit(1)
        
except ImportError:
    print("❌ DuckDB is not installed!")
    print("\nTo install, run:")
    print("  pip install 'duckdb>=1.2.0'")
    sys.exit(1)

