# Duplicate and overlap accounting

Canonical observations retained: 400,000.
Active source-to-observation lineage references: 400,000.
Valid input rows represented by completed/sampled manifest entries: 400,000; invalid input rows recorded: 0.
Exact duplicate observations removed (within-file plus repeated source references): 0 (within-file 0; cross-source 0).
All processed input rows counted including invalid: 400,000.
Potential-overlap groups retained, not removed: 0.

## Exact duplicate criterion

The SHA-256 row identity is computed from normalized date/time, coordinates, sensor, satellite, instrument, product version, native confidence, FRP, native brightness fields, scan/track, day/night and detection type. It excludes file path, filename, source product label and raw JSON provenance. Product version and sensor identity remain part of the key. The canonical observations table contains one row per exact fingerprint; `observation_sources` preserves each contributing file and raw row.

Within-file repeats are counted separately. Cross-source repeats are measured from the active source-lineage table, not by summing historical log events. Different product versions or measurements with the same event signature are potential overlaps and remain separate. Nearby coordinates/times are never sufficient for deletion.

## Current run limits

The initialized warehouse currently contains the legacy leading-row sample, not a full scan of the raw collection. Its exact duplicate count applies only to rows in that sample. Baseline filename/size/CRC32 matches are screening candidates, not proof. During full ingestion a ZIP member is marked `copy_verified` and skipped only when its uncompressed SHA-256 matches the fully scanned disk CSV. File-copy screening is distinct from row-level observation identity.

## Largest potential-overlap groups

No potential-overlap groups were found in the current processed rows.
