# NEXT_STEPS — remaining scientific and scale work

FireScope now retains the existing static dashboard and local HTTP server but moves analytical reads to a DuckDB warehouse. The data work adds a schema/provenance audit, sampled-data bootstrap, incremental CSV/ZIP ingestion, exact-row deduplication, alternate source lineage, invalid-row retention, UTC normalization, Parquet partitions, daily/monthly/yearly/country/H3/grid aggregates, detection-only coverage reporting and a descriptive H3 MODIS/VIIRS comparison. No calibration model is claimed or validated.

## Reproduce the current demo

```sh
cd /workspace
npm install --prefix app
.venv/bin/python -m pip install -r requirements.txt
node src/build_country_boundaries.js
.venv/bin/python src/data_pipeline.py init --root .
.venv/bin/python src/audit_data.py --root .
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python app/server.py
```

The existing processed output reports a 400,000-row sample, not the earlier 60,000 figure. `init` imports that legacy sample into DuckDB; those sources remain `sampled` until the full raw-input ingestion finishes. To incrementally process every discovered local CSV and non-copy ZIP member, run:

```sh
.venv/bin/python src/data_pipeline.py ingest --root .
```

Use `--limit-rows-per-file N` only for an explicitly sampled run. Unchanged `complete` inputs are skipped; `--force` reprocesses. Full local file processing still does not prove complete FIRMS historical coverage.

## Remaining work, in order

1. Run `.venv/bin/python src/data_pipeline.py ingest --root .` to fully process the currently available raw CSVs and ZIP-only members. The initial warehouse was bootstrapped from the legacy 400k sample; raw files not marked `complete` remain unprocessed or sampled.
2. Review `DATA_INVENTORY.csv` copy candidates. Name/size/CRC32 screening is not a cryptographic file proof; all row-level exact duplicates are removed by normalized fingerprints, while source links are retained. Keep archive/NRT/version overlaps visible as potential overlaps.
3. Verify the full-ingest output against original file counts, checksums, product/version/date ranges, invalid rows, duplicate records and every affected Parquet partition. Rerun `ingest` unchanged and confirm it skips complete sources without changing canonical totals.
4. Obtain authoritative sensor acquisition-opportunity metadata if available. Hotspot-only files cannot label absent dates or regions as confirmed zero activity; the current calendar can only show detection-observed coverage.
5. Inspect H3 res-6 matched cell-days and `HARMONIZATION_COMPARISON.csv`. These are descriptive, sampled-input comparisons. Fit a calibration only with adequate provenance-clean overlap, independent spatial/time holdouts and reported MAE/RMSE/correlation/bias; no model has been validated.
6. Run backend API tests against both global aggregates, selected dates, country aggregates and custom polygon AOIs after the full ingest. Verify API totals equal DuckDB tables and map sampling does not change totals.
7. Reassess processing time, DuckDB database size, Parquet partition size and AOI latency on the full collection; compact only affected years and do not reprocess unchanged source files.
8. Keep trend/anomaly statements labeled as observed detections until sensor availability and coverage are defensibly normalized. Do not treat raw count changes as equivalent changes in actual fire incidence.
9. Extend hazard, imagery, burned-area or warning modules only with independently sourced data and validation; none is produced by this data pipeline.

## Known implementation constraints

- The legacy standardized sample contains 400,000 leading rows from 20 archive CSVs, 22 observed dates, and incomplete coverage. The 60,000 figure is not supported by the current quality report.
- The warehouse is initially bootstrapped from that sample; this phase does not claim the large raw archive and all country/year/NRT files were fully ingested. Check `PROCESSING_STATUS.md` and `PROCESSING_MANIFEST.csv` for actual states.
- Dashboard marker samples are bounded to 2,000 points; totals come from DuckDB aggregates. Custom AOIs use exact point-in-polygon filtering; country selections use the local Natural Earth assignment table.
- Detection-only coverage cannot provide confirmed zero-fire days. All dates without detections remain unknown coverage.
- H3 resolution 6 is a common comparison index only. The model validation report records that no calibration model has been trained or validated.
- Raw files are never modified; the legacy demo writer now refuses to replace its existing derived outputs without `--force-output`.
