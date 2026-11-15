# Sensor comparison, spatial analysis and limitations

> Current data pipeline policy, duplicate identity, coverage semantics and harmonization plan are documented in `/workspace/HARMONIZATION_METHODOLOGY.md`, `DATA_AUDIT.md`, and `PROCESSING_STATUS.md`. This file records historical Phase 2 behavior of the earlier dashboard sample; do not use its old ingest-scope and gzip-scan statements as current pipeline status.

## Source scope and processing status

The workspace inventory finds 23 top-level CSV files and 25 ZIP archives. The 20 top-level finalized `fire_archive_*.csv` point-observation files are ingested; three `fire_nrt_*.csv` files are excluded as overlapping near-real-time records. ZIP member copies and country/year MODIS exports are excluded pending a provenance and date-overlap audit. The country/year exports inspected have hotspot coordinates and are point-level observations, not country aggregate statistics.

The currently available processed file is a leading-row demonstration sample: the first 20,000 rows from each of 20 archive files (400,000 records; 160,000 MODIS and 240,000 VIIRS). Its observed acquisition dates span 2018-11-11 to 2025-12-04 but are only 22 distinct dates in this sample. This is not continuous temporal coverage. The report labels input coverage incomplete even if a full pass is later run, because the workspace collection itself is not a complete historical FIRMS archive.

## Normalization

`src/ingest.py` parses FIRMS acquisition date and HHMM as UTC, validates geographic coordinate bounds, labels MODIS Terra/Aqua and VIIRS Suomi-NPP/NOAA-20/NOAA-21, and retains confidence text, FRP, brightness, bright_t31, bright_ti4, bright_ti5, day/night, scan/track, source version/file and product. Native thermal channels are not treated as interchangeable. Raw input files are never overwritten.

## Map and AOI calculations

The frontend uses Leaflet with locally served Natural Earth 1:50m country polygons redistributed via `world-atlas`. Country selection, drawn rectangles/polygons and coordinate-centered circles send GeoJSON geometry to the local analytics service. AOI membership uses Shapely's prepared geometry intersection query where installed (with a boundary-inclusive point-in-polygon fallback). The service streams every processed observation and computes count, FRP, date series, sensor/satellite mix and day/night counts from those rows. It does not use the displayed marker sample for statistics. The point layer is capped at 2,000; the optional density layer summarizes all filtered records into equal-angle 2° cells (displaying up to 5,000 highest-count cells). Those cells are visualization aggregates, not H3.

## Calendar coverage semantics

Calendar cells with rows show measured detections and FRP. Missing cells are labeled `no record / coverage unknown` or `outside processed date span`. FIRMS hotspot-only files do not establish satellite acquisition-opportunity coverage for every day, so no absent row is interpreted as confirmed zero fire. Clicking a date updates the selected-day KPIs and map while leaving the current period trend intact. Date range, sensor family, satellite-layer set, confidence threshold and AOI are shared across the analytics request.

## Sensor comparison and harmonization

The dashboard preserves and compares native MODIS and VIIRS observations. Daily counts and FRP sums are measured aggregates stratified by the selected sensor/satellites. They are not calibrated or converted to a sensor-independent quantity. No individual detections are declared identical, no model is fitted, and no validation metrics are reported.

A defensible next method is to assemble provenance-clean overlapping dates, bin observations to a common UTC-day and spatial grid, retain sensor/satellite counts and FRP separately, match cell-days only where observation support is sufficient, and fit a transparent aggregate calibration if adequate matched data exist. Use time-block holdouts and report sample count, MAE, RMSE, correlation, coverage, and uncertainty. Do not calibrate against fabricated labels or interpret raw count differences as changes in real-world fire incidence.

## Known limitations

- The processed source collection is incomplete; current demo is additionally a leading-row sample.
- The UI's seasonal/yearly trends describe available rows only and are not coverage-adjusted climatology or anomaly products.
- Exact row-level duplicate detection is not complete. Duplicate archive copies are avoided by selecting only the top-level finalized CSVs, but source periods/products still need auditing.
- 1:50m country boundaries are generalized; small islands and complex coastlines are simplified. AOIs use their supplied polygon/circle geometry, not a rectangular country approximation.
- The map uses OpenStreetMap raster tiles and needs network access for the basemap. Analytics/data endpoints remain local.
- Backend summary queries stream the processed compressed CSV and cache a few recent filter combinations. This is acceptable for the present 400k demo but should move to indexed/precomputed Parquet/DuckDB/H3 or equivalent before larger interactive datasets.
- No acquisition opportunity, environmental hazard, fire spread, burned-area, satellite imagery or operational warning data are integrated.
