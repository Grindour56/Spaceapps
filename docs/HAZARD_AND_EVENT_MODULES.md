# Hazard assessment and event imagery module design

## Fire hazard is separate from observed active-fire activity

FireScope's current observations are satellite-reported thermal anomalies. They are not a forecast, hazard probability, ignition prediction, spread perimeter, or burned-area estimate. No hazard score is currently computed.

## Proposed hazard module

A future `src/hazard/` service should ingest environmental inputs independently from FIRMS observations and retain source, spatial/temporal resolution, quality flags and uncertainty. Candidate data families include:

- Weather: air temperature, relative humidity, wind speed/direction and precipitation from ERA5-Land or authoritative national meteorological services.
- Drought/fuel moisture: soil moisture, precipitation deficits and drought indices such as SPEI, with time-window and reanalysis caveats.
- Vegetation/fuels: land cover, vegetation condition, biomass/fuel availability and seasonal phenology from validated MODIS/Sentinel products.
- Terrain and exposure: elevation, slope and aspect, plus historical fire occurrence for calibration and evaluation.

Recommended architecture: a separate `/api/hazard` query and map layer, environmental data ETL into versioned Parquet, AOI/time matching, transparent documented model with uncertainty, and spatiotemporal holdout evaluation. Keep labels and legends explicit: `Observed active-fire detections`, `Environmental conditions`, and `Estimated fire risk` must never be merged into one unlabeled layer. A production alert requires independent operational validation, false-alarm analysis and uncertainty reporting.

## Before/after event explorer

An event explorer should let users select an AOI and dates, then obtain cloud-masked Sentinel-2 or Landsat surface-reflectance scenes; calculate NDVI/NBR/dNBR with clear compositing and cloud/shadow masks; and compare against an appropriate MODIS burned-area product where temporal/spatial resolution allows. It must not infer a burn perimeter from hotspots. Record image product, scene IDs, dates, CRS, processing steps and uncertainty. No imagery ingestion or burned-area analysis is implemented in this MVP, and no imagery is automatically downloaded.
