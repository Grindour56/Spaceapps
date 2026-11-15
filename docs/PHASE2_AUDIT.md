# Phase 2 initial audit

Audit performed before Phase 2 code changes.

## Existing application and reusable parts

- Frontend: one dependency-free static HTML/CSS/JavaScript page at `app/index.html`; no React/Vite project, package.json, Plotly.js, or existing browser state library. Reuse the FireScope branding, responsive dark theme, calendar, time-series bars, sensor summaries, CSV export, and normalized point-field display.
- Backend: `app/server.py` is a Python standard-library HTTP server. It serves only an explicit allowlist (page plus daily CSV, map sample JSON, quality JSON); it has no query API. Preserve this allowlist and extend safely for any new endpoints.
- Pipeline: `src/inventory.py` inventories CSV headers and ZIP directory metadata without extraction. `src/ingest.py` streams top-level finalized `fire_archive_*.csv` and writes normalized gzip observations, daily summaries, map reservoir sample, and quality JSON.
- Data outputs: current files are a demo subset: 20,000 leading rows from each of 20 archive CSVs = 400,000 rows, `partial=true`; sampled dates 2018-11-11 through 2025-12-04. The data are not a representative global time series. Existing daily output has only 22 observed calendar dates. Do not interpret unrepresented days as zero.
- Native source schema: archive point rows include latitude/longitude, brightness, scan/track, acquisition date/time, satellite, instrument, confidence, version, bright_t31, FRP, day/night, type. Product-derived brightness fields remain distinct. No row-level dedupe audit or validated calibration exists.
- Screenshot audit: selected year is 2025, the calendar displays January 2025, the peak card says 2025-12-02, and the daily chart contains only three date bars (Dec 2–4). This is a genuine state/data-coverage mismatch: month default and summary are disconnected, and the chart represents sparse sample days.

## Dependency and capability check

- No package.json, pyproject.toml, requirements file, or frontend package lock was found.
- None of GeoPandas, Shapely, PyProj, DuckDB, PyArrow, H3, FastAPI, Pandas, or Plotly is installed in the Python environment.
- Node/npm exist globally, but no project JS framework/dependencies are configured. No dependencies have been installed during this audit.

The initial audit found no project dependencies. Phase 2 then added only the packages needed for its selected approach: Leaflet 1.9.4, world-atlas 2.0.2, topojson-client 3.1.0 and Shapely 2.2.0. The backend now has an allowlisted POST analytics API that scans the complete processed file for filtered aggregates; current 400k demonstration queries measured about 6–8 seconds, so larger full ingests need indexed storage/precomputation.

The screenshot mismatch is fixed by selecting the month of the latest sampled acquisition date at startup (December 2025 for the current sample) and using one shared state object for year/month/date/range/sensor/satellite/confidence/AOI. Empty dates remain coverage-unknown, not zero.

