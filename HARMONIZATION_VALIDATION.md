# Harmonization validation report

## Verdict

No MODIS-to-VIIRS calibration model has been trained or validated. No calibrated estimates are published.

## Data available to this phase

- Canonical observations currently in the warehouse: 400,000.
- Distinct detection-observed dates in the bootstrapped sample: 22.
- Source-manifest statuses: `{"sampled": 20}`.
- MODIS/VIIRS H3 cell-day pairs with both sensors, descriptive only: 13,439.
- The legacy sample consists of leading rows from archive files, not a representative or complete time series.

## Validation metrics

MAE: not computed; RMSE: not computed; correlation: not computed; bias/residual distributions: not computed. Training, validation and test splits were not created. The available input has no independent reference labels or acquisition-opportunity coverage metadata, and the processed sample is not suitable for claiming predictive or calibration performance.

## What is available

`HARMONIZATION_COMPARISON.csv` and DuckDB table `sensor_comparison` summarize MODIS and VIIRS detection counts and FRP separately by UTC day and H3 resolution-6 cell where both sensors have records. These are observed products on a shared index, not equivalent measurements and not calibrated estimates.

## Next validation gate

Complete raw-file provenance and overlap audit; establish adequate matched cell-day support; define an independent reference target; use spatial and contiguous temporal holdouts; compare against a documented baseline; report sample count, MAE, RMSE, correlation, bias, residual distributions and performance by region/platform. Do not fit a model if those conditions are unmet.
