# FireScope — global active-fire exploration MVP
Project URL: https://spaceapps-zeta.vercel.app

FireScope extends the existing FIRMS data workflow with a local Leaflet map, Natural Earth country selection, polygon/rectangle/circle AOIs, synchronized date and sensor filters, an interactive daily calendar, regional summaries, annual/monthly trend views, and filtered daily CSV export.

## Setup and run

From `/workspace` (manual FIRMS acquisition is unchanged):

```sh
npm install --prefix app
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
node src/build_country_boundaries.js
.venv/bin/python src/data_pipeline.py init --root .
.venv/bin/python src/audit_data.py --root .
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python app/server.py
```

Open `http://127.0.0.1:8000`. Map tiles come from OpenStreetMap, so the basemap requires internet access. Country polygons, application code and processed observations are served locally. The server binds to loopback and serves only explicit allowlisted files and the analytics endpoint; it does not serve raw source files or `map_key`.

To process all newly discovered raw CSVs and ZIP-only members incrementally (large; allow substantial time and disk):

```sh
.venv/bin/python src/data_pipeline.py ingest --root .
```

The pipeline skips unchanged complete inputs using a path/size/mtime fingerprint and stores a SHA-256 while fully scanning each source. `--limit-rows-per-file N` is for explicit samples only; it remains marked sampled. `--force` requests reprocessing. The local collection is not certified as the complete FIRMS history, so full local processing does not mean complete historical coverage.

## Existing stack and dependencies

The frontend remains vanilla HTML/CSS/JavaScript; the backend remains the standard-library Python HTTP server. The application uses Leaflet, bundled `world-atlas` Natural Earth 1:50m boundaries and Shapely AOI membership. The data warehouse uses DuckDB, Parquet output and H3 resolution 6 for shared spatial aggregation. Python and JavaScript dependencies are pinned in their project manifests. PyArrow, GeoPandas, Plotly and FastAPI are not required by this implementation.

## Data and interpretation

`src/audit_data.py` creates `DATA_INVENTORY.csv`, `DATA_AUDIT.md` and `PROCESSING_STATUS.md`; ZIP members are inventoried without extraction. `src/data_pipeline.py` discovers raw CSVs and ZIP members, validates and standardizes point records, and stores source lineage in `warehouse/firescope.duckdb`. It keeps archive, NRT and country/year inputs distinct and only skips a ZIP member as a file copy after its uncompressed SHA-256 matches a fully scanned disk CSV; the inventory labels weaker name/size/CRC matches as candidates.

The existing `src/ingest.py` remains as a legacy demo-sample generator and refuses to overwrite its outputs unless `--force-output` is explicit. The current pipeline adds:

- `warehouse/firescope.duckdb` — canonical unique observations, source manifest, alternate provenance, invalid rows, duplicate accounting, coverage, and precomputed activity/comparison tables.
- `data/processed/parquet/year=YYYY/observations.parquet` — yearly Parquet partitions; sensor and satellite remain columns, avoiding small-file fragmentation.
- `data/processed/aggregates/` — daily/monthly/annual, country, H3, 2° display-grid, coverage and sensor-comparison Parquet aggregates.
- `DATA_INVENTORY.csv`, `DATA_AUDIT.md`, `PROCESSING_STATUS.md`, `DATA_QUALITY_REPORT.json`, `COVERAGE_REPORT.csv`, `DUPLICATE_REPORT.md`, and `PROCESSING_MANIFEST.csv`.

The on-disk legacy demo output is a non-representative leading sample of 20,000 rows per archive file: 400,000 records from 20 archive files, with sampled dates November 11, 2018 through December 4, 2025 and only 22 observed dates. It does not match the previously quoted 60,000-record figure. The new warehouse bootstraps those rows as `legacy_standardized_sample`; its manifest keeps them marked sampled until full raw ingestion replaces them. H3 cell/day counts support sensor comparisons; the display map keeps its simpler 2° grid. Visual marker sampling never changes analytical totals. A blank date is unknown coverage, not confirmed zero activity.

Global summaries and country selections query DuckDB aggregates; custom AOIs filter candidate point rows in DuckDB and use exact Shapely point-in-polygon membership. The server does not scan compressed observations for routine global queries. Calendar, metrics, sensor/satellite mix and charts share request filters. Clicking a day changes date-specific KPIs and map points while the time-series remains the selected range. Draw polygon, drag rectangle and coordinate/radius circle remain supported.

Native MODIS `brightness`/`bright_t31` and VIIRS `bright_ti4`/`bright_ti5` are separate columns. H3 resolution 6 is a shared comparison grid, not a claim of sensor equivalence. No calibrated sensor-independent fire count is produced; no model has been validated. No anomaly claim, hazard score, burned-area estimate, imagery analysis or operational prediction is implemented.

## Validation

Run the test suite:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

The local API reads the DuckDB warehouse and precomputed aggregates, with bounded deterministic point samples and exact AOI fallback. Coverage remains detection-only: no acquisition opportunity metadata or confirmed-zero periods are available. See `DATA_AUDIT.md`, `PROCESSING_STATUS.md`, `DUPLICATE_REPORT.md`, `HARMONIZATION_METHODOLOGY.md`, `HARMONIZATION_VALIDATION.md`, and `NEXT_STEPS.md`.
