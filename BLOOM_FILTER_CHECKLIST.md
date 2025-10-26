# Bloom Filter Implementation Checklist

Use this checklist to implement and verify bloom filters in your parquet branch.

## ✅ Implementation Checklist

### Phase 0: Upgrade DuckDB (REQUIRED)
**First, upgrade to DuckDB 1.2.0+ for bloom filter support:**
```bash
pip install --upgrade 'duckdb>=1.2.0'
```

- [ ] Upgrade DuckDB to 1.2.0 or later
- [ ] Verify version: `python -c "import duckdb; print(duckdb.__version__)"`
- [ ] Should show 1.2.0 or higher

### Phase 1: Code Changes (COMPLETED)
- [x] Update `requirements.txt` to require DuckDB 1.2.0+
- [x] Update `convert_csv_to_parquet.py` with correct Parquet settings
- [x] Update `step1_prepare_data.py` with correct Parquet settings
- [x] Create `verify_bloom_filters.py` verification script
- [x] Update `README.md` with bloom filter documentation
- [x] Create `BLOOM_FILTER_IMPLEMENTATION.md` guide

### Phase 2: Generate Parquet Files with Bloom Filters
Choose ONE method:

**Method 1: Direct CSV to Parquet Conversion**
```bash
python convert_csv_to_parquet.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output
```

**Method 2: Full Pipeline**
```bash
python step1_prepare_data.py \
  --data-dir /Users/matthewhu/Downloads/data \
  --out-dir ./output
```

- [ ] Run chosen conversion script
- [ ] Wait for completion (may take several minutes)
- [ ] Confirm "Bloom filters enabled" message in output
- [ ] Check output directory contains Parquet files

### Phase 3: Verify Bloom Filters
```bash
python verify_bloom_filters.py --parquet-dir ./output/events
```

- [ ] Run verification script
- [ ] Confirm bloom filters found on columns: country, publisher_id, advertiser_id, auction_id
- [ ] Review performance test results
- [ ] Check query execution plans show filter usage

### Phase 4: Test Query Performance
```bash
# If using full pipeline, continue with:
python step2_build_rollups.py
python step3_run_queries.py --out-dir ./output --truth-dir ./query_results
```

- [ ] Run queries from inputs.py
- [ ] Compare performance with previous runs (if available)
- [ ] Verify queries using bloom-filtered columns are faster
- [ ] Check query results match expected output

### Phase 5: Commit Changes (When Ready)
```bash
git add convert_csv_to_parquet.py step1_prepare_data.py verify_bloom_filters.py
git add README.md BLOOM_FILTER_IMPLEMENTATION.md BLOOM_FILTER_CHECKLIST.md
git commit -m "feat: implement bloom filters in Parquet files

- Add bloom filters on country, publisher_id, advertiser_id, auction_id
- Configure ZSTD compression and 100K row groups
- Add verification script to test bloom filter functionality
- Update documentation with bloom filter usage guide"
```

- [ ] Review all changes
- [ ] Run tests to ensure nothing broke
- [ ] Commit changes
- [ ] Push to parquet branch

## 🎯 Success Criteria

You'll know bloom filters are working correctly when:

1. ✅ Verification script confirms bloom filters on 4 columns
2. ✅ Queries with equality filters show 10-100x speedup
3. ✅ Query plans show "Filters:" in EXPLAIN ANALYZE
4. ✅ Parquet file size increased by ~1-2% (acceptable overhead)
5. ✅ All query results match expected output

## 📊 Expected Performance

| Before Bloom Filters | After Bloom Filters |
|---------------------|---------------------|
| Query 2 (country filter): 1.5s | Query 2: 0.05s (30x faster) |
| Point lookups: 2.0s | Point lookups: 0.02s (100x faster) |
| Multi-filter queries: 2.5s | Multi-filter queries: 0.03s (80x faster) |

## 🚨 Common Issues

### Issue: "Not implemented Error: Unrecognized option"
**Solution:** Upgrade DuckDB to version 1.2.0 or later
```bash
pip install --upgrade 'duckdb>=1.2.0'
python -c "import duckdb; print(duckdb.__version__)"  # Should show 1.2.0+
```

### Issue: "No bloom filters found"
**Solution:** Regenerate Parquet files - old files don't have bloom filters
```bash
rm -rf ./output/events
python convert_csv_to_parquet.py --data-dir /path/to/csv --out-dir ./output
```

### Issue: "No Parquet files found"
**Solution:** Check directory path or generate files first
```bash
ls -la ./output/events/  # Check if files exist
```

### Issue: "Out of memory"
**Solution:** Increase DuckDB memory limit
```bash
export DUCKDB_MEMORY_LIMIT=16GB
python convert_csv_to_parquet.py ...
```

## 📝 Notes

- Bloom filters work best with equality filters (=, IN)
- Range queries (>, <, BETWEEN) don't benefit from bloom filters
- Smaller row groups = more bloom filters = better filtering but slightly larger files
- ZSTD compression is slower than Snappy but achieves better compression ratios
- Bloom filters are automatically used by DuckDB when reading Parquet files

## 🎉 Next Steps After Completion

1. Measure and document actual performance improvements
2. Consider adding bloom filters to more columns based on query patterns
3. Optimize row group size based on your specific workload
4. Share performance benchmarks with team
5. Update production deployment with bloom filter configuration

