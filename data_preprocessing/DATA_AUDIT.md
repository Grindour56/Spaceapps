# FireScope data audit

Audit root: `/workspace/Projects/spaceapp`. Raw data were not modified; no FIRMS API or data download was used.

## Available inputs

- 1257 disk CSVs (23,233,129,508 bytes), 25 ZIP archives (5,110,822,617 bytes).
- Filename/path classification: `{"archive": 20, "country_year": 1234, "nrt": 3}`.
- 1257 CSV headers contain both latitude and longitude and are point-observation schemas. Other schemas are marked summary/unknown, never coerced to point detections.
- Distinct disk-CSV schemas and file counts: `{"latitude|longitude|brightness|scan|track|acq_date|acq_time|satellite|instrument|confidence|version|bright_t31|frp|daynight": 3, "latitude|longitude|brightness|scan|track|acq_date|acq_time|satellite|instrument|confidence|version|bright_t31|frp|daynight|type": 1254}`. The inventory also profiles only the first 20 data rows/file for sample types, missingness, satellites, versions and sample dates; those sample dates are not full-file coverage.
- `DATA_INVENTORY.csv` lists every disk CSV, ZIP archive and CSV archive member with its header schema, processing state and copy-candidate notes.

## Existing processing

- Current legacy output is `400,000` valid rows from 20 finalized archive files, sampled at 20,000 leading rows/file. The requested 60,000 figure does not match the current quality report.
- Sensor split: `{"MODIS": 160000, "VIIRS": 240000}`; detected-row span 2018-11-11–2025-12-04, not continuous coverage.
- Existing producer: `src/ingest.py`; it selects only top-level `fire_archive_*.csv`, writes `observations.csv.gz`, daily activity, map sample and quality JSON. The quality report says exact row duplicates were not audited. `src/inventory.py` produced the prior JSON inventory.
- NRT, ZIP members and country/year exports were excluded from the legacy sample. Country/year files are point-level only where the actual schema has coordinates; no country aggregate is promoted to point data.
- The output sample omitted original raw-row JSON. The new full ingest stores all original fields as JSON and keeps sensor-native brightness columns distinct.

## New DuckDB snapshot

- Canonical warehouse rows: 400000; sensors `{"MODIS": 160000, "VIIRS": 240000}`; satellites `{"Aqua": 103174, "NOAA-20": 100000, "NOAA-21": 40000, "Suomi-NPP": 100000, "Terra": 56826}`.
- Exact duplicate rows removed: 0; source lineage is retained in `observation_sources`. Potential overlap groups retained without deletion: 0.
- Coverage rows 193854 mean detections observed only. No acquisition-opportunity feed is available, so missing dates are unknown, not zero.
- Database date summary: ('2018-11-11', '2025-12-04', 22).
