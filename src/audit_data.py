"""FIRMS dataset audit and provenance reports; credential files are never scanned."""
from __future__ import annotations
import csv, hashlib, json, os, zipfile
from collections import Counter, defaultdict
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from data_pipeline import discover_inputs, file_fingerprint, _classify_source, connect_database, duplicate_accounting


def _header(path):
    try:
        with Path(path).open('r',encoding='utf-8-sig',newline='') as f: return next(csv.reader(f),[])
    except Exception as e: return [f'HEADER_ERROR:{type(e).__name__}']
def _zip_header(zf,name):
    try:
        with zf.open(name) as f: return next(csv.reader([f.readline().decode('utf-8-sig',errors='replace')]),[])
    except Exception as e: return [f'HEADER_ERROR:{type(e).__name__}']
def _profile_reader(reader,limit=20):
    rows=[];missing=Counter();types=defaultdict(set);dates=[];satellites=set();instruments=set();versions=set()
    for row in reader:
        rows.append(row)
        for key,value in row.items():
            value=(value or '').strip()
            if not value:missing[key]+=1;types[key].add('missing');continue
            if key.lower() in {'acq_date','date','acquisition_date'}:types[key].add('date');dates.append(value[:10])
            else:
                try:float(value);types[key].add('numeric')
                except ValueError:types[key].add('text')
            if key.lower()=='satellite':satellites.add(value)
            if key.lower()=='instrument':instruments.add(value)
            if key.lower()=='version':versions.add(value)
        if len(rows)>=limit:break
    return {'sample_data_rows':len(rows),'sample_date_min':min(dates) if dates else '', 'sample_date_max':max(dates) if dates else '',
      'sample_missing_json':json.dumps(dict(missing),sort_keys=True,separators=(',',':')),
      'sample_types_json':json.dumps({k:sorted(v) for k,v in sorted(types.items())},sort_keys=True,separators=(',',':')),
      'sample_satellites':'|'.join(sorted(satellites)),'sample_instruments':'|'.join(sorted(instruments)),'sample_versions':'|'.join(sorted(versions))}

def _csv_profile(path):
    try:
        with Path(path).open('r',encoding='utf-8-sig',newline='') as f:return _profile_reader(csv.DictReader(f))
    except Exception as e:return {'sample_data_rows':0,'sample_missing_json':'{}','sample_types_json':json.dumps({'error':type(e).__name__})}

def _zip_profile(zf,member):
    try:
        import io
        with zf.open(member) as b:
            with io.TextIOWrapper(b,encoding='utf-8-sig',newline='') as f:return _profile_reader(csv.DictReader(f))
    except Exception as e:return {'sample_data_rows':0,'sample_missing_json':'{}','sample_types_json':json.dumps({'error':type(e).__name__})}

def _schema(cols): return hashlib.sha256('\x1f'.join(cols).encode()).hexdigest() if cols else ''
def _kind(cols): return 'point_observations' if {'latitude','longitude'}<={c.strip().lower() for c in cols} else 'summary_or_unknown_schema'
def _notes(cols, sample_instruments=''):
    c={x.strip().lower() for x in cols};n=[];inst=(sample_instruments or '').upper()
    if {'brightness','bright_t31'}&c:
        if 'VIIRS' in inst or 'SNPP' in inst:
            n.append('FIRMS brightness/bright_t31 column names on VIIRS export (kept sensor-distinct from MODIS)')
        else:
            n.append('MODIS-native brightness field(s)')
    if {'bright_ti4','bright_ti5'}&c:n.append('VIIRS-native brightness field(s)')
    return '; '.join(n)

def build_inventory(root,output=None,db_path=None):
    root=Path(root).resolve();output=Path(output or root/'DATA_INVENTORY.csv');db_path=Path(db_path or root/'warehouse'/'firescope.duckdb')
    csvs,zips=discover_inputs(root);manifest={}
    if db_path.is_file():
        con=connect_database(db_path)
        manifest={r[0]:r[1:] for r in con.execute('SELECT source_path,status,input_records,valid_records,invalid_records,exact_duplicates,source_fingerprint,content_sha256,file_crc32 FROM source_manifest').fetchall()};con.close()
    rows=[];direct=defaultdict(list)
    for p in csvs:direct[p.name].append(p)
    for p in csvs:
        rel=p.relative_to(root).as_posix();cols=_header(p);profile=_csv_profile(p);m=manifest.get(rel);fp,_,mtime=file_fingerprint(p)
        rows.append({'record_type':'raw_csv','path':rel,'source_class':_classify_source(rel),'observation_type':_kind(cols),'size_bytes':p.stat().st_size,'mtime_ns':mtime,'header_columns':'|'.join(cols),'schema_sha256':_schema(cols),'records_seen':m[1] if m else '','valid_records':m[2] if m else '','invalid_records':m[3] if m else '','exact_duplicates':m[4] if m else '','processing_status':m[0] if m else 'not_processed','input_fingerprint':fp,'checksum_sha256':m[6] if m else '','notes':_notes(cols,profile.get('sample_instruments','')),**profile})
    for zp in zips:
        rel=zp.relative_to(root).as_posix();rows.append({'record_type':'raw_zip','path':rel,'source_class':'zip_archive','observation_type':'container','size_bytes':zp.stat().st_size,'mtime_ns':zp.stat().st_mtime_ns,'processing_status':'inventoried_not_extracted','input_fingerprint':file_fingerprint(zp)[0],'notes':'Central directory inspected; raw archive unchanged.'})
        try:
            with zipfile.ZipFile(zp) as zf:
                for info in zf.infolist():
                    if info.is_dir() or not info.filename.lower().endswith('.csv'):continue
                    cols=_zip_header(zf,info.filename);profile=_zip_profile(zf,info.filename);cands=[p for p in direct.get(Path(info.filename).name,[]) if p.stat().st_size==info.file_size]
                    candidate=f"same_name_size:{cands[0].relative_to(root).as_posix()}" if cands else ''
                    member=f'{rel}!{info.filename}';m=manifest.get(member)
                    rows.append({'record_type':'zip_csv_member','path':member,'source_class':_classify_source(member),'observation_type':_kind(cols),'size_bytes':info.file_size,'header_columns':'|'.join(cols),'schema_sha256':_schema(cols),'records_seen':m[1] if m else '','valid_records':m[2] if m else '','invalid_records':m[3] if m else '','exact_duplicates':m[4] if m else '','processing_status':m[0] if m else ('copy_candidate_unverified' if candidate else 'not_processed'),'input_fingerprint':'','checksum_sha256':'','duplicate_candidate':candidate,'notes':f'ZIP CRC32={info.CRC:08x}; name/size is a candidate, not exact proof. {_notes(cols,profile.get("sample_instruments",""))}',**profile})
        except zipfile.BadZipFile: rows.append({'record_type':'raw_zip_error','path':rel,'processing_status':'invalid_zip'})
    quality_path=root/'outputs'/'quality_report.json';q=json.loads(quality_path.read_text()) if quality_path.is_file() else {}
    legacy=root/'outputs'/'observations.csv.gz'
    rows.append({'record_type':'legacy_processed_output','path':'outputs/observations.csv.gz','source_class':'standardized_sample','observation_type':'point_observations','size_bytes':legacy.stat().st_size if legacy.exists() else '','records_seen':q.get('raw_rows',''),'valid_records':q.get('valid_rows',''),'invalid_records':q.get('invalid_rows',''),'exact_duplicates':q.get('exact_duplicates',''),'processing_status':'sampled','notes':'Legacy 20,000-leading-rows-per-file sample; not collection-complete.'})
    for folder in (root/'warehouse',root/'data'/'processed',root/'outputs'):
        if folder.exists():
            for p in folder.rglob('*'):
                if p.is_file() and not p.name.endswith('.wal') and '.staging' not in p.parts: rows.append({'record_type':'derived_output','path':p.relative_to(root).as_posix(),'source_class':'derived','observation_type':'database' if p.suffix=='.duckdb' else 'parquet' if p.suffix=='.parquet' else 'report_or_export','size_bytes':p.stat().st_size,'processing_status':'created_or_existing','notes':'Derived artifact; not a raw input.'})
    for name in ('COVERAGE_REPORT.csv','PROCESSING_MANIFEST.csv','DUPLICATE_RECORDS.csv','HARMONIZATION_COMPARISON.csv','DATA_QUALITY_REPORT.json','DATA_AUDIT.md','PROCESSING_STATUS.md','DUPLICATE_REPORT.md','HARMONIZATION_VALIDATION.md','HARMONIZATION_METHODOLOGY.md','NEXT_STEPS.md'):
        p=root/name
        if p.is_file():rows.append({'record_type':'derived_output','path':name,'source_class':'derived_report','observation_type':'report_or_export','size_bytes':p.stat().st_size,'processing_status':'created_or_existing','notes':'Generated project report or analytical export.'})
    fields=['record_type','path','source_class','observation_type','size_bytes','mtime_ns','header_columns','schema_sha256','records_seen','valid_records','invalid_records','exact_duplicates','processing_status','input_fingerprint','checksum_sha256','duplicate_candidate','sample_data_rows','sample_date_min','sample_date_max','sample_missing_json','sample_types_json','sample_satellites','sample_instruments','sample_versions','notes']
    with output.open('w',newline='',encoding='utf-8') as f:w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    return {'path':str(output),'rows':len(rows),'disk_csvs':len(csvs),'zip_archives':len(zips),'zip_csv_members':sum(x['record_type']=='zip_csv_member' for x in rows)}

def _summary(root,db_path):
    root=Path(root).resolve();csvs,zips=discover_inputs(root);classes=Counter(_classify_source(p.relative_to(root).as_posix()) for p in csvs)
    qfile=root/'outputs'/'quality_report.json';q=json.loads(qfile.read_text()) if qfile.is_file() else {}
    schemas=Counter('|'.join(_header(p)) for p in csvs)
    x={'root':str(root),'raw_csv_count':len(csvs),'raw_csv_bytes':sum(p.stat().st_size for p in csvs),'zip_count':len(zips),'zip_bytes':sum(p.stat().st_size for p in zips),'csv_classes':dict(classes),'point_csv_count':sum(_kind(_header(p))=='point_observations' for p in csvs),'schema_counts':dict(schemas),'legacy_quality':q}
    db=Path(db_path)
    if db.exists():
        con=connect_database(db);x['warehouse']={'records':con.execute('SELECT count(*) FROM observations').fetchone()[0],'by_sensor':dict(con.execute('SELECT sensor,count(*) FROM observations GROUP BY sensor').fetchall()),'by_satellite':dict(con.execute('SELECT satellite,count(*) FROM observations GROUP BY satellite').fetchall()),'date_range':con.execute('SELECT cast(min(acquisition_date) AS VARCHAR),cast(max(acquisition_date) AS VARCHAR),count(distinct acquisition_date) FROM observations').fetchone(),'manifest':dict(con.execute('SELECT status,count(*) FROM source_manifest GROUP BY status').fetchall()),'manifest_records':con.execute('SELECT count(*) FROM source_manifest').fetchone()[0],'exact_duplicates':duplicate_accounting(con)['exact_duplicates_removed'],'potential_overlap_groups':con.execute('SELECT count(*) FROM potential_overlap_groups').fetchone()[0],'coverage_rows':con.execute('SELECT count(*) FROM coverage_report').fetchone()[0] if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='coverage_report'").fetchone()[0] else 0,'invalid_rows':con.execute('SELECT count(*) FROM invalid_observations').fetchone()[0]};con.close()
    return x

def write_duplicate_report(root,db_path):
    root=Path(root).resolve();db=Path(db_path);con=connect_database(db)
    by_kind=dict(con.execute('SELECT duplicate_kind,count(*),coalesce(sum(duplicate_count),0) FROM duplicate_records GROUP BY duplicate_kind').fetchall())
    accounting=duplicate_accounting(con)
    input_rows,valid_rows,invalid_rows=con.execute("SELECT coalesce(sum(input_records),0),coalesce(sum(valid_records),0),coalesce(sum(invalid_records),0) FROM source_manifest WHERE status IN ('complete','sampled')").fetchone()
    retained=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    refs=con.execute('SELECT count(*) FROM observation_sources').fetchone()[0]
    ambiguity=con.execute('SELECT count(*) FROM potential_overlap_groups').fetchone()[0]
    examples=con.execute('SELECT sensor,satellite,date_min,date_max,observation_count,versions,sources FROM potential_overlap_groups ORDER BY observation_count DESC,date_min LIMIT 20').fetchall()
    con.close()
    lines=['# Duplicate and overlap accounting','',f'Canonical observations retained: {retained:,}.',f'Active source-to-observation lineage references: {refs:,}.',f'Valid input rows represented by completed/sampled manifest entries: {valid_rows:,}; invalid input rows recorded: {invalid_rows:,}.',f'Exact duplicate observations removed (within-file plus repeated source references): {accounting["exact_duplicates_removed"]:,} (within-file {accounting["within_file_duplicates"]:,}; cross-source {accounting["cross_source_duplicates"]:,}).',f'All processed input rows counted including invalid: {input_rows:,}.',f'Potential-overlap groups retained, not removed: {ambiguity:,}.','', '## Exact duplicate criterion','', 'The SHA-256 row identity is computed from normalized date/time, coordinates, sensor, satellite, instrument, product version, native confidence, FRP, native brightness fields, scan/track, day/night and detection type. It excludes file path, filename, source product label and raw JSON provenance. Product version and sensor identity remain part of the key. The canonical observations table contains one row per exact fingerprint; `observation_sources` preserves each contributing file and raw row.','', 'Within-file repeats are counted separately. Cross-source repeats are measured from the active source-lineage table, not by summing historical log events. Different product versions or measurements with the same event signature are potential overlaps and remain separate. Nearby coordinates/times are never sufficient for deletion.','', '## Current run limits','', 'The initialized warehouse currently contains the legacy leading-row sample, not a full scan of the raw collection. Its exact duplicate count applies only to rows in that sample. Baseline filename/size/CRC32 matches are screening candidates, not proof. During full ingestion a ZIP member is marked `copy_verified` and skipped only when its uncompressed SHA-256 matches the fully scanned disk CSV. File-copy screening is distinct from row-level observation identity.','', '## Largest potential-overlap groups','']
    if examples:
        lines.append('| Sensor | Satellite | Date range | Fingerprints | Versions | Sources |')
        lines.append('|---|---|---|---:|---|---|')
        for sensor,sat,dmin,dmax,n,versions,sources in examples:
            lines.append(f'| {sensor} | {sat} | {dmin}–{dmax} | {n} | {versions} | {str(sources)[:240]} |')
    else: lines.append('No potential-overlap groups were found in the current processed rows.')
    (root/'DUPLICATE_REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return {'exact_duplicates_removed':accounting['exact_duplicates_removed'],'potential_overlap_groups':ambiguity,'report':str(root/'DUPLICATE_REPORT.md')}

def write_quality_report(root,db_path):
    import duckdb,h3,shapely
    root=Path(root).resolve();con=connect_database(db_path)
    total=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    lo,hi,days=con.execute('SELECT min(acquisition_date),max(acquisition_date),count(DISTINCT acquisition_date) FROM observations').fetchone()
    latmin,latmax,lonmin,lonmax=con.execute('SELECT min(latitude),max(latitude),min(longitude),max(longitude) FROM observations').fetchone()
    by_sensor=[]
    for row in con.execute("SELECT sensor,count(*),count(*) FILTER(WHERE acquisition_datetime_utc IS NULL),count(*) FILTER(WHERE frp IS NULL),count(*) FILTER(WHERE confidence_raw IS NULL),count(*) FILTER(WHERE daynight NOT IN ('D','N') OR daynight IS NULL),count(*) FILTER(WHERE sensor='MODIS' AND brightness IS NULL),count(*) FILTER(WHERE sensor='MODIS' AND bright_t31 IS NULL),count(*) FILTER(WHERE sensor='VIIRS' AND bright_ti4 IS NULL),count(*) FILTER(WHERE sensor='VIIRS' AND bright_ti5 IS NULL) FROM observations GROUP BY sensor ORDER BY sensor").fetchall():
        by_sensor.append(dict(zip(['sensor','records','missing_datetime','missing_frp','missing_confidence','unknown_daynight','missing_modis_brightness','missing_bright_t31','missing_bright_ti4','missing_bright_ti5'],row)))
    invalid=dict(con.execute('SELECT validation_errors,count(*) FROM invalid_observations GROUP BY validation_errors').fetchall())
    manifest=dict(con.execute('SELECT status,count(*) FROM source_manifest GROUP BY status').fetchall())
    processed=con.execute("SELECT coalesce(sum(input_records),0),coalesce(sum(valid_records),0),coalesce(sum(invalid_records),0) FROM source_manifest WHERE status IN ('complete','sampled')").fetchone()
    duplicates=duplicate_accounting(con)
    result={'report_version':'firescope-data-quality-1.0','processing_version':'firescope-pipeline-1.1','warehouse':str(Path(db_path).resolve()),'canonical_observations':total,'manifest_input_rows_read':processed[0],'manifest_valid_rows':processed[1],'manifest_invalid_rows':processed[2],'invalid_row_reasons':invalid,'sensor_quality':by_sensor,'satellite_records':dict(con.execute('SELECT satellite,count(*) FROM observations GROUP BY satellite').fetchall()),'product_versions':dict(con.execute("SELECT coalesce(product_version,'unknown'),count(*) FROM observations GROUP BY 1").fetchall()),'date_min':str(lo) if lo else None,'date_max':str(hi) if hi else None,'distinct_detection_dates':days,'geographic_bounds':{'latitude_min':latmin,'latitude_max':latmax,'longitude_min':lonmin,'longitude_max':lonmax},'duplicates':duplicates,'potential_overlap_groups':con.execute('SELECT count(*) FROM potential_overlap_groups').fetchone()[0],'country_assignments':con.execute('SELECT count(*) FROM country_assignment').fetchone()[0],'manifest_status':manifest,'input_coverage_complete':False,'coverage_semantics':'Detection-observed only; no confirmed zero-detection periods are generated.','h3_resolution':6,'processing_parameters':{'timestamp':'FIRMS acquisition date + HHMM interpreted as UTC; original values retained','coordinates':'latitude [-90,90], longitude [-180,180]; invalid rows retained in invalid_observations','confidence':'native field retained; UI bands are a display convention, not calibrated cross-sensor confidence','brightness':'MODIS brightness/bright_t31 and VIIRS bright_ti4/bright_ti5 remain separate','duplicate_identity':'SHA-256 over normalized event fields; source path/product label excluded; product version and sensor retained','provenance':'one canonical row per exact fingerprint; all source associations retained'},'software_versions':{'duckdb':duckdb.__version__,'h3':getattr(h3,'__version__','unknown'),'shapely':shapely.__version__}}
    con.close();path=root/'DATA_QUALITY_REPORT.json';path.write_text(json.dumps(result,indent=2,default=str),encoding='utf-8')
    return {'path':str(path),'canonical_observations':total,'invalid_records':processed[2],'duplicates':duplicates}

def write_validation_report(root,db_path):
    root=Path(root).resolve();con=connect_database(db_path)
    records=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    dates=con.execute('SELECT count(DISTINCT acquisition_date) FROM observations').fetchone()[0]
    matched=con.execute('SELECT count(*) FROM sensor_comparison').fetchone()[0] if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='sensor_comparison'").fetchone()[0] else 0
    manifest=dict(con.execute('SELECT status,count(*) FROM source_manifest GROUP BY status').fetchall())
    con.close()
    body=("# Harmonization validation report\n\n## Verdict\n\nNo MODIS-to-VIIRS calibration model has been trained or validated. No calibrated estimates are published.\n\n## Data available to this phase\n\n"+
      f"- Canonical observations currently in the warehouse: {records:,}.\n- Distinct detection-observed dates in the bootstrapped sample: {dates:,}.\n- Source-manifest statuses: `{json.dumps(manifest,sort_keys=True)}`.\n- MODIS/VIIRS H3 cell-day pairs with both sensors, descriptive only: {matched:,}.\n- The legacy sample consists of leading rows from archive files, not a representative or complete time series.\n\n## Validation metrics\n\nMAE: not computed; RMSE: not computed; correlation: not computed; bias/residual distributions: not computed. Training, validation and test splits were not created. The available input has no independent reference labels or acquisition-opportunity coverage metadata, and the processed sample is not suitable for claiming predictive or calibration performance.\n\n## What is available\n\n`HARMONIZATION_COMPARISON.csv` and DuckDB table `sensor_comparison` summarize MODIS and VIIRS detection counts and FRP separately by UTC day and H3 resolution-6 cell where both sensors have records. These are observed products on a shared index, not equivalent measurements and not calibrated estimates.\n\n## Next validation gate\n\nComplete raw-file provenance and overlap audit; establish adequate matched cell-day support; define an independent reference target; use spatial and contiguous temporal holdouts; compare against a documented baseline; report sample count, MAE, RMSE, correlation, bias, residual distributions and performance by region/platform. Do not fit a model if those conditions are unmet.\n")
    (root/'HARMONIZATION_VALIDATION.md').write_text(body,encoding='utf-8')
    return {'matched_cell_days':matched,'model_validated':False,'report':str(root/'HARMONIZATION_VALIDATION.md')}

def write_reports(root,db_path):
    root=Path(root).resolve();s=_summary(root,db_path);q=s['legacy_quality'];w=s.get('warehouse',{})
    csvs,_=discover_inputs(root);archives=sum(_classify_source(p.relative_to(root).as_posix())=='archive' for p in csvs);nrt=sum(_classify_source(p.relative_to(root).as_posix())=='nrt' for p in csvs);country=sum(_classify_source(p.relative_to(root).as_posix())=='country_year' for p in csvs)
    audit=(f"# FireScope data audit\n\nAudit root: `{root}`. Raw data were not modified; no FIRMS API or data download was used.\n\n## Available inputs\n\n- {s['raw_csv_count']} disk CSVs ({s['raw_csv_bytes']:,} bytes), {s['zip_count']} ZIP archives ({s['zip_bytes']:,} bytes).\n- Filename/path classification: `{json.dumps(s['csv_classes'],sort_keys=True)}`.\n- {s['point_csv_count']} CSV headers contain both latitude and longitude and are point-observation schemas. Other schemas are marked summary/unknown, never coerced to point detections.\n- Distinct disk-CSV schemas and file counts: `{json.dumps(s['schema_counts'],sort_keys=True)}`. The inventory also profiles only the first 20 data rows/file for sample types, missingness, satellites, versions and sample dates; those sample dates are not full-file coverage.\n- `DATA_INVENTORY.csv` lists every disk CSV, ZIP archive and CSV archive member with its header schema, processing state and copy-candidate notes.\n\n## Existing processing\n\n- Current legacy output is `{q.get('valid_rows',0):,}` valid rows from {q.get('files_seen',0)} finalized archive files, sampled at 20,000 leading rows/file. The requested 60,000 figure does not match the current quality report.\n- Sensor split: `{json.dumps(q.get('sensors',{}),sort_keys=True)}`; detected-row span {q.get('date_min')}–{q.get('date_max')}, not continuous coverage.\n- Existing producer: `src/ingest.py`; it selects only top-level `fire_archive_*.csv`, writes `observations.csv.gz`, daily activity, map sample and quality JSON. The quality report says exact row duplicates were not audited. `src/inventory.py` produced the prior JSON inventory.\n- NRT, ZIP members and country/year exports were excluded from the legacy sample. Country/year files are point-level only where the actual schema has coordinates; no country aggregate is promoted to point data.\n- The output sample omitted original raw-row JSON. The new full ingest stores all original fields as JSON and keeps sensor-native brightness columns distinct.\n\n## New DuckDB snapshot\n\n- Canonical warehouse rows: {w.get('records','not initialized')}; sensors `{json.dumps(w.get('by_sensor',{}),sort_keys=True)}`; satellites `{json.dumps(w.get('by_satellite',{}),sort_keys=True)}`.\n- Exact duplicate rows removed: {w.get('exact_duplicates','not audited')}; source lineage is retained in `observation_sources`. Potential overlap groups retained without deletion: {w.get('potential_overlap_groups','not computed')}.\n- Coverage rows {w.get('coverage_rows','not computed')} mean detections observed only. No acquisition-opportunity feed is available, so missing dates are unknown, not zero.\n- Database date summary: {w.get('date_range','not initialized')}.\n")
    (root/'DATA_AUDIT.md').write_text(audit,encoding='utf-8')
    status=(f"# FireScope processing status\n\nWarehouse: `{root/'warehouse'/'firescope.duckdb'}`.\n\n## Processed\n\n- Legacy standardized sample: {q.get('valid_rows',0):,} rows, {q.get('files_seen',0)} finalized archive sources, 20,000 leading rows/source. Sampled, not full-file ingestion.\n- DuckDB canonical rows after exact duplicate removal: {w.get('records','not initialized')}; all bootstrapped records are labeled `legacy_standardized_sample` because original raw rows were not retained in the old output.\n- Manifest status counts: `{json.dumps(w.get('manifest',{}),sort_keys=True)}`. Only `complete` indicates a full scan.\n\n## Available but not fully processed\n\n- Current disk sources: {s['raw_csv_count']} CSVs ({archives} archive, {nrt} NRT, {country} country/year) plus {s['zip_count']} ZIPs. Check `DATA_INVENTORY.csv` for each file's status.\n- ZIP members matching an already processed disk CSV by basename, size and CRC32 are streamed to compare uncompressed SHA-256. Only matching SHA-256 is marked `copy_verified` and skipped; name/size/CRC-only matches remain candidates or are processed.\n- `--limit-rows-per-file N` is explicitly sampled. `--force` requests reprocessing; unchanged complete files are skipped by path/size/mtime fingerprint. An edit that preserves path, size and mtime requires `--force`; full SHA-256 is recorded during complete scans.\n- Coverage opportunity/confirmed-zero metadata is unavailable. No harmonization calibration model has been trained or validated.\n")
    (root/'PROCESSING_STATUS.md').write_text(status,encoding='utf-8')
    duplicates=write_duplicate_report(root,db_path)
    validation=write_validation_report(root,db_path)
    quality=write_quality_report(root,db_path)
    inv=build_inventory(root,output=root/'DATA_INVENTORY.csv',db_path=db_path)
    return {'inventory':inv,'warehouse':w,'duplicate_report':duplicates,'validation_report':validation,'quality_report':quality,'audit':str(root/'DATA_AUDIT.md'),'processing_status':str(root/'PROCESSING_STATUS.md')}

if __name__=='__main__':
    import argparse
    ap=argparse.ArgumentParser();ap.add_argument('--root',default='.');ap.add_argument('--db',default='warehouse/firescope.duckdb');args=ap.parse_args()
    root=Path(args.root).resolve();db=Path(args.db);db=db if db.is_absolute() else root/db
    print(json.dumps(write_reports(root,db),indent=2,default=str))
