# FireScope processing status

Warehouse: `/workspace/Projects/spaceapp/warehouse/firescope.duckdb`.

## Processed

- Legacy standardized sample: 400,000 rows, 20 finalized archive sources, 20,000 leading rows/source. Sampled, not full-file ingestion.
- DuckDB canonical rows after exact duplicate removal: 400000; all bootstrapped records are labeled `legacy_standardized_sample` because original raw rows were not retained in the old output.
- Manifest status counts: `{"sampled": 20}`. Only `complete` indicates a full scan.

## Available but not fully processed

- Current disk sources: 1257 CSVs (20 archive, 3 NRT, 1234 country/year) plus 25 ZIPs. Check `DATA_INVENTORY.csv` for each file's status.
- ZIP members matching an already processed disk CSV by basename, size and CRC32 are streamed to compare uncompressed SHA-256. Only matching SHA-256 is marked `copy_verified` and skipped; name/size/CRC-only matches remain candidates or are processed.
- `--limit-rows-per-file N` is explicitly sampled. `--force` requests reprocessing; unchanged complete files are skipped by path/size/mtime fingerprint. An edit that preserves path, size and mtime requires `--force`; full SHA-256 is recorded during complete scans.
- Coverage opportunity/confirmed-zero metadata is unavailable. No harmonization calibration model has been trained or validated.
