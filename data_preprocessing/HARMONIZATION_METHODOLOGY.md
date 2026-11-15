# Harmonization methodology and status

## Status

The warehouse supports a sensor-aware descriptive comparison; it does not produce a calibrated sensor-independent fire count. The legacy records currently used to bootstrap it are a non-representative leading-row sample. No harmonization model is claimed.

## Native measurement preservation

Each standardized observation keeps sensor, satellite, instrument, product version, source product, original acquisition date/time, normalized UTC date/time, scan, track, confidence as received, numeric confidence where parseable, FRP, day/night, detection type, and all original input columns in `raw_record_json` for newly ingested raw rows. The legacy processed CSV did not preserve the full original source row, so bootstrapped legacy records are labeled `legacy_standardized_sample` and have no recoverable raw-row JSON.

MODIS brightness and `bright_t31` remain separate from VIIRS `bright_ti4` and `bright_ti5`. The native confidence representation is retained. The dashboard's `confidence_band` is a display/filter convenience: categorical VIIRS values retain low/nominal/high labels; numeric MODIS confidence is grouped into `<30`, `30–<80`, and `>=80` bins. These UI bins are not a claim that confidence scores are scientifically equivalent across sensors or products.

NASA FIRMS product descriptions identify MODIS confidence on a 0–100 scale and VIIRS confidence as low/nominal/high classes; the VIIRS product description also distinguishes native I4/I5 brightness fields. FIRMS documentation notes that standard archived products replace NRT data as they become available, so archive/NRT temporal overlap is expected and must retain product provenance.

## Common spatial-temporal comparison

- Temporal unit: UTC acquisition calendar day.
- Spatial comparison key: H3 resolution 6 (`h3_cell`), with native observations and sensor/satellite labels retained. H3 cells are not equal-area; the published average area at resolution 6 is about 36.1 km² and individual cell area varies. Resolution 6 is a starting comparison scale, not a scientific optimum. Re-run sensitivity checks at coarser/finer resolutions before interpreting results.
- Aggregates: detection count, FRP sum/mean/median, missing FRP, sensor/satellite split, confidence-band counts, and day/night counts.
- Display grid: legacy equal-angle 2° cells are retained only for the dashboard density layer. They are not used as an equal-area scientific grid.

MODIS and VIIRS cell-day aggregates are comparable summaries of observations mapped to a common index, not equivalent measurements. Different native footprints, detection sensitivities, spectral channels, confidence schemes, and observation opportunities remain relevant. A shared H3 cell does not make two detections the same event.

## Duplicate and overlap policy

An exact normalized-row fingerprint includes UTC/date/time, coordinates, sensor, satellite, instrument, source product/version, confidence, FRP, native brightness fields, scan/track, day/night, and detection type. The source filename/path and raw JSON provenance are excluded from the row identity, so the same normalized observation in repeated source files is retained once in `observations` and all source paths remain in `observation_sources`.

A separate potential-overlap signature uses sensor/satellite, acquisition time/date, exact coordinates, scan/track and detection type while omitting product version and measured values. Groups with a shared signature but different exact fingerprints are reported and retained. Coordinate/time proximity alone never removes a detection. Archive/NRT and product-version differences remain visible for audit.

## Coverage semantics

`COVERAGE_REPORT.csv` and `coverage_report` contain only date/sensor/satellite/version/H3 cells with observed detections. No zero-detection coverage rows are synthesized. FIRMS hotspot rows alone do not establish that a satellite observed a location on every blank date. Blank periods therefore remain unknown, not confirmed zero activity.

## Calibration pathway (not yet executed)

If provenance-clean overlaps and opportunity metadata support it:
1. Aggregate by UTC day and H3 cell while keeping MODIS and each VIIRS platform separate.
2. Select matched cell-days with explicit minimum support; report which sensors actually contributed observations.
3. Compare counts and FRP with descriptive bias/residual summaries before fitting.
4. Hold out contiguous time blocks and geographic regions; do not randomly split neighboring observations across train/test.
5. Compare a transparent baseline with linear or robust aggregate calibration; report n, MAE, RMSE, correlation, bias, residual distributions, and performance by region/platform.
6. Publish calibrated estimates separately from raw observations and label uncertainty and coverage.

## Reference material

NASA FIRMS, “MODIS Fire Hotspots” product description; NASA FIRMS, “VIIRS Fire Hotspots” product description; NASA Earthdata, “Collection 2 Visible Infrared Imaging Radiometer Suite (VIIRS) 375 m Active Fire User Guide”; H3 documentation, “Table of Cell Areas for H3 Resolutions” and “Indexing functions.”
