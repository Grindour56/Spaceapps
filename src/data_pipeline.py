"""Incremental, provenance-preserving FIRMS ingestion and DuckDB warehouse."""
from __future__ import annotations
import csv, datetime as dt, hashlib, io, json, math, os, re, tempfile, time, uuid, zipfile, zlib
from collections import Counter, defaultdict
from pathlib import Path

import duckdb
import h3

PROCESSING_VERSION = "firescope-pipeline-1.1"
H3_RESOLUTION = 6
OBS_FIELDS = [
    "row_fingerprint","overlap_signature","acquisition_datetime_utc","acquisition_date",
    "acquisition_time_original","original_acq_date","original_acq_time","latitude","longitude",
    "sensor","satellite","instrument","product_version","source_product","confidence_raw",
    "confidence_numeric","confidence_class","confidence_band","frp","brightness","bright_t31",
    "bright_ti4","bright_ti5","scan","track","daynight","detection_type","source_file",
    "source_path","source_fingerprint","raw_record_json","quality_flags","h3_cell","provenance_level"
]
OBS_TYPES = {
    "row_fingerprint":"VARCHAR","overlap_signature":"VARCHAR","acquisition_datetime_utc":"TIMESTAMP",
    "acquisition_date":"DATE","acquisition_time_original":"VARCHAR","original_acq_date":"VARCHAR",
    "original_acq_time":"VARCHAR","latitude":"DOUBLE","longitude":"DOUBLE","sensor":"VARCHAR",
    "satellite":"VARCHAR","instrument":"VARCHAR","product_version":"VARCHAR","source_product":"VARCHAR",
    "confidence_raw":"VARCHAR","confidence_numeric":"DOUBLE","confidence_class":"VARCHAR",
    "confidence_band":"VARCHAR","frp":"DOUBLE","brightness":"DOUBLE","bright_t31":"DOUBLE",
    "bright_ti4":"DOUBLE","bright_ti5":"DOUBLE","scan":"DOUBLE","track":"DOUBLE","daynight":"VARCHAR",
    "detection_type":"VARCHAR","source_file":"VARCHAR","source_path":"VARCHAR","source_fingerprint":"VARCHAR",
    "raw_record_json":"VARCHAR","quality_flags":"VARCHAR","h3_cell":"VARCHAR","provenance_level":"VARCHAR"
}
EXCLUDED_DIRS={".git",".venv","node_modules","__pycache__","outputs","warehouse","processed","data","app","docs","src","tests",".pytest_cache"}
EXCLUDED_CSV_NAMES={"DATA_INVENTORY.csv","COVERAGE_REPORT.csv","PROCESSING_MANIFEST.csv","DUPLICATE_RECORDS.csv","HARMONIZATION_COMPARISON.csv"}
SATELLITE_MAP={"TERRA":"Terra","AQUA":"Aqua","SNPP":"Suomi-NPP","NPP":"Suomi-NPP","N20":"NOAA-20","NOAA20":"NOAA-20","J1V":"NOAA-20","N21":"NOAA-21","NOAA21":"NOAA-21","J2V":"NOAA-21"}

def _num(value):
    try:
        x=float(value)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None

def _classify_source(source_path, record=None):
    name=Path(source_path.split("!",1)[-1]).name.lower()
    low=source_path.lower()
    if name.startswith("fire_nrt_") or "nrt" in str((record or {}).get('version','')).lower(): return "nrt"
    if name.startswith("fire_archive_"): return "archive"
    if re.search(r"/modis/20\d\d/",low) or name.startswith("modis_20"): return "country_year"
    if "!" in source_path: return "zip_member"
    return "other_csv"

def _canonical_json(obj):
    return json.dumps(obj,sort_keys=True,separators=(",",":"),ensure_ascii=False,default=str)

def _digest(values):
    return hashlib.sha256(_canonical_json(values).encode("utf-8")).hexdigest()

def normalize_observation(raw, source_path, source_fingerprint="", provenance_level="raw"):
    """Normalize one point row; return (record, errors). Invalid rows remain reportable."""
    original={str(k).strip():v for k,v in raw.items() if k is not None}
    r={k.lower():v for k,v in original.items()}
    errors=[]; flags=[]
    lat=_num(r.get("latitude",r.get("lat"))); lon=_num(r.get("longitude",r.get("lon")))
    if lat is None: errors.append("latitude_missing_or_non_numeric")
    elif not -90<=lat<=90: errors.append("latitude_out_of_range")
    if lon is None: errors.append("longitude_missing_or_non_numeric")
    elif not -180<=lon<=180: errors.append("longitude_out_of_range")
    raw_date=str(r.get("acq_date",r.get("acquisition_date",r.get("date",""))) or "").strip()
    raw_time=str(r.get("acq_time",r.get("acquisition_time",r.get("time",""))) or "").strip()
    acq_date=None; acq_time=raw_time or None; acq_dt=None
    if not raw_date: errors.append("acquisition_date_missing")
    else:
        try: acq_date=dt.date.fromisoformat(raw_date[:10]).isoformat()
        except ValueError: errors.append("acquisition_date_invalid")
    if not raw_time: flags.append("acquisition_time_missing")
    elif acq_date:
        t=raw_time.zfill(4)
        if not t[:4].isdigit() or len(t)>4: errors.append("acquisition_time_invalid")
        else:
            try:
                parsed=dt.datetime.strptime(acq_date+t[:4],"%Y-%m-%d%H%M").replace(tzinfo=dt.timezone.utc)
                acq_dt=parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError: errors.append("acquisition_time_invalid")
    inst=str(r.get("instrument","") or "").strip()
    satellite_raw=str(r.get("satellite","") or "").strip()
    filename=Path(source_path.split("!",1)[-1]).name
    sat=SATELLITE_MAP.get(satellite_raw.upper(),satellite_raw or "Unknown")
    inst_up=inst.upper()
    sensor="VIIRS" if inst_up in {"VIIRS","SNPP","NPP"} or sat in {"Suomi-NPP","NOAA-20","NOAA-21"} else "MODIS" if inst_up=="MODIS" or sat in {"Terra","Aqua"} else "UNKNOWN"
    if sensor=="UNKNOWN": flags.append("sensor_unresolved")
    version=str(r.get("version","") or "").strip() or None
    source_product=_classify_source(source_path,r)
    conf_raw=None if r.get("confidence") in (None,"") else str(r.get("confidence"))
    conf_num=_num(conf_raw)
    conf_text=(conf_raw or "").strip().lower()
    conf_class=conf_text if conf_text in {"l","n","h","low","nominal","high"} else ("numeric" if conf_num is not None else "unknown")
    if conf_num is not None:
        conf_band="low" if conf_num<30 else "nominal" if conf_num<80 else "high"
        if not 0<=conf_num<=100: flags.append("numeric_confidence_outside_0_100")
    elif conf_class in {"l","low"}: conf_band="low"
    elif conf_class in {"n","nominal"}: conf_band="nominal"
    elif conf_class in {"h","high"}: conf_band="high"
    else: conf_band="unknown"
    frp=_num(r.get("frp")); brightness=_num(r.get("brightness")); b31=_num(r.get("bright_t31"))
    ti4=_num(r.get("bright_ti4")); ti5=_num(r.get("bright_ti5"))
    scan=_num(r.get("scan")); track=_num(r.get("track"))
    if frp is None: flags.append("frp_missing_or_non_numeric")
    elif frp<0: flags.append("frp_negative")
    if conf_raw is None: flags.append("confidence_missing")
    if brightness is None and b31 is None and ti4 is None and ti5 is None: flags.append("brightness_fields_missing")
    daynight=str(r.get("daynight","") or "").strip().upper() or None
    if daynight not in {"D","N"}: flags.append("daynight_unknown")
    dtype=None if r.get("type") in (None,"") else str(r.get("type"))
    raw_json=_canonical_json(original)
    rec={
        "acquisition_datetime_utc":acq_dt,"acquisition_date":acq_date,"acquisition_time_original":raw_time or None,
        "original_acq_date":raw_date or None,"original_acq_time":raw_time or None,"latitude":lat,"longitude":lon,
        "sensor":sensor,"satellite":sat,"instrument":inst or None,"product_version":version,"source_product":source_product,
        "confidence_raw":conf_raw,"confidence_numeric":conf_num,"confidence_class":conf_class,"confidence_band":conf_band,
        "frp":frp,"brightness":brightness,"bright_t31":b31,"bright_ti4":ti4,"bright_ti5":ti5,"scan":scan,"track":track,
        "daynight":daynight,"detection_type":dtype,"source_file":filename,"source_path":source_path,
        "source_fingerprint":source_fingerprint or None,"raw_record_json":raw_json,"quality_flags":_canonical_json(flags),
        "h3_cell":h3.latlng_to_cell(lat,lon,H3_RESOLUTION) if lat is not None and lon is not None and -90<=lat<=90 and -180<=lon<=180 else None,
        "provenance_level":provenance_level,
    }
    if errors: return None,errors
    rec["row_fingerprint"]=exact_fingerprint(rec)
    rec["overlap_signature"]=overlap_signature(rec)
    return rec,[]

def exact_fingerprint(row):
    """Exact normalized-row identity; excludes file/product provenance but retains version and native values."""
    keys=("acquisition_datetime_utc","acquisition_date","acquisition_time_original","latitude","longitude","sensor","satellite","instrument","product_version","confidence_raw","frp","brightness","bright_t31","bright_ti4","bright_ti5","scan","track","daynight","detection_type")
    return _digest({k:row.get(k) for k in keys})

def overlap_signature(row):
    """Candidate same-observation key across products/versions; never used to delete rows."""
    keys=("acquisition_datetime_utc","acquisition_date","acquisition_time_original","latitude","longitude","sensor","satellite","scan","track","daynight","detection_type")
    return _digest({k:row.get(k) for k in keys})

def file_fingerprint(path):
    p=Path(path);st=p.stat()
    return hashlib.sha256(f"{p.resolve().as_posix()}\0{st.st_size}\0{st.st_mtime_ns}".encode()).hexdigest(),st.st_size,st.st_mtime_ns

def connect_database(db_path):
    p=Path(db_path);p.parent.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(str(p));con.execute("SET TimeZone='UTC'");ensure_schema(con);return con

def ensure_schema(con):
    cols=",\n".join(f'"{k}" {v}' for k,v in OBS_TYPES.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS observations ({cols}, PRIMARY KEY(row_fingerprint))")
    con.execute("CREATE TABLE IF NOT EXISTS source_manifest(source_path VARCHAR PRIMARY KEY, source_fingerprint VARCHAR, fingerprint_kind VARCHAR, content_sha256 VARCHAR, file_crc32 UBIGINT, source_class VARCHAR, processing_version VARCHAR, status VARCHAR, started_at TIMESTAMP, completed_at TIMESTAMP, input_records BIGINT, valid_records BIGINT, invalid_records BIGINT, exact_duplicates BIGINT, retained_records BIGINT, new_canonical_records BIGINT, potential_overlap_rows BIGINT, sample_limit BIGINT, missing_json VARCHAR, output_location VARCHAR, error VARCHAR, notes VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS observation_sources(row_fingerprint VARCHAR, source_path VARCHAR, source_fingerprint VARCHAR, source_file VARCHAR, source_product VARCHAR, raw_record_json VARCHAR, PRIMARY KEY(row_fingerprint,source_path))")
    con.execute("CREATE TABLE IF NOT EXISTS duplicate_records(source_path VARCHAR, row_fingerprint VARCHAR, duplicate_kind VARCHAR, duplicate_count BIGINT, previous_source_paths VARCHAR, detected_at TIMESTAMP DEFAULT current_timestamp)")
    con.execute("CREATE TABLE IF NOT EXISTS invalid_observations(source_path VARCHAR, source_fingerprint VARCHAR, row_number BIGINT, raw_record_json VARCHAR, validation_errors VARCHAR, recorded_at TIMESTAMP DEFAULT current_timestamp, PRIMARY KEY(source_path,source_fingerprint,row_number))")
    con.execute("CREATE TABLE IF NOT EXISTS potential_overlap_groups(overlap_signature VARCHAR PRIMARY KEY, observation_count BIGINT, sensor VARCHAR, satellite VARCHAR, date_min DATE, date_max DATE, versions VARCHAR, sources VARCHAR, updated_at TIMESTAMP DEFAULT current_timestamp)")
    con.execute("CREATE TABLE IF NOT EXISTS country_assignment(row_fingerprint VARCHAR PRIMARY KEY, country_id VARCHAR, country_name VARCHAR, boundary_version VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs(run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP, completed_at TIMESTAMP, status VARCHAR, source_count BIGINT, records_added BIGINT, error VARCHAR)")
    for tbl in ("observations","source_manifest","duplicate_records","invalid_observations","potential_overlap_groups","pipeline_runs"):
        for _,col,ctype,*_ in con.execute(f"PRAGMA table_info('{tbl}')").fetchall():
            if str(ctype).upper()=="TIMESTAMP WITH TIME ZONE":
                con.execute(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET DATA TYPE TIMESTAMP USING {col} AT TIME ZONE 'UTC'")

def _row_values(rec):
    return [rec.get(k) for k in OBS_FIELDS]

def _cleanup_source(con,source_path):
    con.execute("DELETE FROM observation_sources WHERE source_path=?",[source_path])
    con.execute("DELETE FROM invalid_observations WHERE source_path=?",[source_path])
    con.execute("DELETE FROM duplicate_records WHERE source_path=?",[source_path])
    con.execute("UPDATE observations AS o SET source_path=s.source_path,source_file=s.source_file,source_product=s.source_product,source_fingerprint=s.source_fingerprint,raw_record_json=s.raw_record_json FROM (SELECT row_fingerprint,source_path,source_file,source_product,source_fingerprint,raw_record_json,row_number() OVER(PARTITION BY row_fingerprint ORDER BY source_path) AS rn FROM observation_sources) s WHERE s.rn=1 AND s.row_fingerprint=o.row_fingerprint AND o.source_path=?",[source_path])
    con.execute("DELETE FROM observations WHERE NOT EXISTS (SELECT 1 FROM observation_sources s WHERE s.row_fingerprint=observations.row_fingerprint)")
    con.execute("DELETE FROM country_assignment WHERE NOT EXISTS (SELECT 1 FROM observations o WHERE o.row_fingerprint=country_assignment.row_fingerprint)")

def duplicate_accounting(con):
    within=int(con.execute("SELECT coalesce(sum(duplicate_count),0) FROM duplicate_records WHERE duplicate_kind='exact_within_file'").fetchone()[0])
    cross=int(con.execute("SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) AS n FROM observation_sources GROUP BY row_fingerprint HAVING count(*)>1)").fetchone()[0])
    return {"exact_duplicates_removed":within+cross,"within_file_duplicates":within,"cross_source_duplicates":cross}

def merge_records(con,source_path,fingerprint,records,*,source_class=None,force=False,sample_limit=None,invalid_records=None,content_sha256=None,file_crc32=None,missing=None,output_location=None,fingerprint_kind="caller_supplied_fingerprint"):
    """Idempotently merge a bounded iterable of normalized rows and update its manifest."""
    ensure_schema(con);source_path=str(source_path);source_class=source_class or _classify_source(source_path)
    previous=con.execute("SELECT source_fingerprint,status,sample_limit,processing_version FROM source_manifest WHERE source_path=?",[source_path]).fetchone()
    if previous and previous[0]==fingerprint and previous[1]=="complete" and previous[3]==PROCESSING_VERSION and not force:
        return {"skipped":True,"source_path":source_path,"status":"complete"}
    if previous and previous[0]==fingerprint and previous[1]=="sampled" and previous[2]==sample_limit and previous[3]==PROCESSING_VERSION and sample_limit is not None and not force:
        return {"skipped":True,"source_path":source_path,"status":"sampled"}
    now=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    con.execute("INSERT INTO source_manifest(source_path,source_fingerprint,fingerprint_kind,content_sha256,file_crc32,source_class,processing_version,status,started_at,sample_limit,output_location,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_path) DO UPDATE SET source_fingerprint=excluded.source_fingerprint,fingerprint_kind=excluded.fingerprint_kind,content_sha256=excluded.content_sha256,file_crc32=excluded.file_crc32,source_class=excluded.source_class,processing_version=excluded.processing_version,status='processing',started_at=excluded.started_at,sample_limit=excluded.sample_limit,output_location=excluded.output_location,error=NULL,notes=excluded.notes",
      [source_path,fingerprint,fingerprint_kind,content_sha256,file_crc32,source_class,PROCESSING_VERSION,"processing",now,sample_limit,output_location,None])
    valid_rows=list(records); invalid_records=invalid_records or []
    with tempfile.TemporaryDirectory(prefix="firescope-merge-") as tmp:
        stage_path=Path(tmp)/"valid.csv";invalid_path=Path(tmp)/"invalid.csv"
        with stage_path.open("w",newline="",encoding="utf-8") as sf:
            sw=csv.DictWriter(sf,fieldnames=OBS_FIELDS);sw.writeheader()
            for r in valid_rows: sw.writerow({k:r.get(k) for k in OBS_FIELDS})
        with invalid_path.open("w",newline="",encoding="utf-8") as inf:
            iw=csv.DictWriter(inf,fieldnames=["source_path","source_fingerprint","row_number","raw_record_json","validation_errors"]);iw.writeheader()
            for item in invalid_records:
                iw.writerow({"source_path":source_path,"source_fingerprint":fingerprint,"row_number":item["row_number"],"raw_record_json":item["raw_record_json"],"validation_errors":item["validation_errors"]})
        try:
            return _merge_stage_file(con,source_path,fingerprint,stage_path,invalid_path,source_class=source_class,source_sha256=content_sha256,file_crc32=file_crc32,input_count=len(valid_rows)+len(invalid_records),valid_count=len(valid_rows),invalid_count=len(invalid_records),sample_limit=sample_limit,missing=missing or {},output_location=output_location,force=force)
        except Exception:
            con.execute("UPDATE source_manifest SET status='failed',error='merge_failed' WHERE source_path=?",[source_path])
            raise

def _text_lines(binary_stream,sha,crc_box):
    first=True
    for line in binary_stream:
        sha.update(line);crc_box[0]=zlib.crc32(line,crc_box[0])
        yield line.decode("utf-8-sig" if first else "utf-8",errors="replace")
        first=False

def _manifest_fingerprint(con,source_path):
    return con.execute("SELECT source_fingerprint,status,sample_limit,processing_version FROM source_manifest WHERE source_path=?",[source_path]).fetchone()

def _mark_processing(con,source_path,fingerprint,source_class,sample_limit,output_location):
    con.execute("INSERT INTO source_manifest(source_path,source_fingerprint,fingerprint_kind,source_class,processing_version,status,started_at,sample_limit,output_location) VALUES(?,?,?,?,?,'processing',current_timestamp,?,?) ON CONFLICT(source_path) DO UPDATE SET source_fingerprint=excluded.source_fingerprint,fingerprint_kind=excluded.fingerprint_kind,source_class=excluded.source_class,processing_version=excluded.processing_version,status='processing',started_at=excluded.started_at,sample_limit=excluded.sample_limit,output_location=excluded.output_location,error=NULL",[source_path,fingerprint,"sha256(path|size|mtime_ns)",source_class,PROCESSING_VERSION,sample_limit,output_location])

def _merge_stage_file(con,source_path,fingerprint,stage_path,invalid_path,*,source_class,source_sha256,file_crc32,input_count,valid_count,invalid_count,sample_limit,missing,output_location,force=False):
    previous=_manifest_fingerprint(con,source_path)
    if previous: _cleanup_source(con,source_path)
    con.execute("CREATE OR REPLACE TEMP TABLE stage AS SELECT * FROM read_csv_auto(?,header=true,all_varchar=true,nullstr='')",[str(stage_path)])
    stage_count=con.execute("SELECT count(*) FROM stage").fetchone()[0]
    distinct_count=con.execute("SELECT count(DISTINCT row_fingerprint) FROM stage").fetchone()[0]
    within_duplicates=stage_count-distinct_count
    con.execute("CREATE OR REPLACE TEMP TABLE stage_unique AS SELECT * FROM stage QUALIFY row_number() OVER(PARTITION BY row_fingerprint ORDER BY source_path)=1")
    across_duplicates=con.execute("SELECT count(*) FROM stage_unique s JOIN observations o USING(row_fingerprint)").fetchone()[0]
    new_count=distinct_count-across_duplicates
    overlap_count=con.execute("SELECT count(DISTINCT s.row_fingerprint) FROM stage_unique s WHERE EXISTS(SELECT 1 FROM observations o WHERE o.overlap_signature=s.overlap_signature AND o.row_fingerprint<>s.row_fingerprint) OR EXISTS(SELECT 1 FROM stage_unique s2 WHERE s2.overlap_signature=s.overlap_signature AND s2.row_fingerprint<>s.row_fingerprint)").fetchone()[0]
    con.execute("BEGIN TRANSACTION")
    try:
        if within_duplicates:
            con.execute("INSERT INTO duplicate_records(source_path,row_fingerprint,duplicate_kind,duplicate_count,previous_source_paths) SELECT ?,row_fingerprint,'exact_within_file',count(*)-1,? FROM stage GROUP BY row_fingerprint HAVING count(*)>1",[source_path,source_path])
        if across_duplicates:
            con.execute("INSERT INTO duplicate_records(source_path,row_fingerprint,duplicate_kind,duplicate_count,previous_source_paths) SELECT ?,s.row_fingerprint,'exact_cross_source',1,(SELECT string_agg(DISTINCT source_path,',') FROM observation_sources os WHERE os.row_fingerprint=s.row_fingerprint) FROM stage_unique s JOIN observations o USING(row_fingerprint)",[source_path])
        con.execute("INSERT INTO observation_sources SELECT DISTINCT row_fingerprint,source_path,source_fingerprint,source_file,source_product,raw_record_json FROM stage_unique")
        sel=[]
        for field in OBS_FIELDS:
            if field in {"acquisition_datetime_utc"}: sel.append(f"try_cast(nullif({field},'') AS TIMESTAMP) AS {field}")
            elif field in {"acquisition_date"}: sel.append(f"try_cast(nullif({field},'') AS DATE) AS {field}")
            elif field in {"latitude","longitude","confidence_numeric","frp","brightness","bright_t31","bright_ti4","bright_ti5","scan","track"}: sel.append(f"try_cast(nullif({field},'') AS DOUBLE) AS {field}")
            else: sel.append(field)
        con.execute(f"INSERT INTO observations({','.join(OBS_FIELDS)}) SELECT {','.join(sel)} FROM stage_unique s WHERE NOT EXISTS(SELECT 1 FROM observations o WHERE o.row_fingerprint=s.row_fingerprint)")
        if invalid_count and invalid_path and Path(invalid_path).stat().st_size:
            con.execute("CREATE OR REPLACE TEMP TABLE invalid_stage AS SELECT * FROM read_csv_auto(?,header=true,all_varchar=true,nullstr='')",[str(invalid_path)])
            con.execute("INSERT INTO invalid_observations SELECT source_path,source_fingerprint,try_cast(row_number AS BIGINT),raw_record_json,validation_errors,current_timestamp FROM invalid_stage ON CONFLICT DO NOTHING")
        status="sampled" if sample_limit is not None else "complete"
        con.execute("UPDATE source_manifest SET content_sha256=?,file_crc32=?,status=?,completed_at=current_timestamp,input_records=?,valid_records=?,invalid_records=?,exact_duplicates=?,retained_records=?,new_canonical_records=?,potential_overlap_rows=?,missing_json=?,error=NULL,output_location=? WHERE source_path=?",[source_sha256,file_crc32,status,input_count,valid_count,invalid_count,within_duplicates+across_duplicates,distinct_count,new_count,overlap_count,_canonical_json(missing),output_location,source_path])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK");raise
    finally:
        for t in ("stage","stage_unique","invalid_stage"): con.execute(f"DROP TABLE IF EXISTS {t}")
    return {"skipped":False,"source_path":source_path,"status":status,"input_records":input_count,"valid_records":valid_count,"invalid_records":invalid_count,"exact_duplicates":within_duplicates+across_duplicates,"retained_records":distinct_count,"new_canonical_records":new_count,"potential_overlap_rows":overlap_count,"sha256":source_sha256}

def process_stream(binary_stream,source_path,db_path,*,file_fingerprint_value,file_size,source_class=None,limit_rows=None,force=False,output_location=None):
    """Stream a CSV (including a ZIP member stream) to a temporary standardized stage, then merge atomically."""
    source_path=str(source_path);source_class=source_class or _classify_source(source_path)
    con=connect_database(db_path)
    prev=_manifest_fingerprint(con,source_path)
    if prev and prev[0]==file_fingerprint_value and prev[1]=="complete" and prev[3]==PROCESSING_VERSION and limit_rows is None and not force:
        con.close();return {"skipped":True,"source_path":source_path,"status":"complete"}
    if prev and prev[0]==file_fingerprint_value and prev[1]=="sampled" and prev[2]==limit_rows and prev[3]==PROCESSING_VERSION and limit_rows is not None and not force:
        con.close();return {"skipped":True,"source_path":source_path,"status":"sampled"}
    _mark_processing(con,source_path,file_fingerprint_value,source_class,limit_rows,output_location)
    stage_dir=Path(db_path).parent/".staging";stage_dir.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="firescope-",dir=stage_dir) as tmp:
        stage_path=Path(tmp)/"valid.csv";invalid_path=Path(tmp)/"invalid.csv"
        sha=hashlib.sha256();crc=[0];missing=Counter();input_count=valid_count=invalid_count=0
        with stage_path.open("w",newline="",encoding="utf-8") as sf, invalid_path.open("w",newline="",encoding="utf-8") as inf:
            sw=csv.DictWriter(sf,fieldnames=OBS_FIELDS);sw.writeheader()
            iw=csv.DictWriter(inf,fieldnames=["source_path","source_fingerprint","row_number","raw_record_json","validation_errors"]);iw.writeheader()
            reader=csv.DictReader(_text_lines(binary_stream,sha,crc))
            for rowno,raw in enumerate(reader,start=1):
                if limit_rows is not None and input_count>=limit_rows: break
                input_count+=1
                record,errors=normalize_observation(raw,source_path,file_fingerprint_value)
                if errors:
                    invalid_count+=1;iw.writerow({"source_path":source_path,"source_fingerprint":file_fingerprint_value,"row_number":rowno,"raw_record_json":_canonical_json(raw),"validation_errors":_canonical_json(errors)})
                else:
                    valid_count+=1
                    for flag in json.loads(record["quality_flags"]): missing[flag]+=1
                    sw.writerow({k:record.get(k) for k in OBS_FIELDS})
        content_sha=sha.hexdigest() if limit_rows is None else None
        crc32=crc[0] if limit_rows is None else None
        try:
            result=_merge_stage_file(con,source_path,file_fingerprint_value,stage_path,invalid_path,source_class=source_class,source_sha256=content_sha,file_crc32=crc32,input_count=input_count,valid_count=valid_count,invalid_count=invalid_count,sample_limit=limit_rows,missing=missing,output_location=output_location,force=force)
        except Exception as exc:
            con.execute("UPDATE source_manifest SET status='failed',error=? WHERE source_path=?",[str(exc)[:1000],source_path]);con.close();raise
    con.close();return result

def process_csv(path,db_path,*,root=None,limit_rows=None,force=False,output_location=None):
    p=Path(path);root=Path(root or p.parent)
    source_path=p.relative_to(root).as_posix() if p.is_relative_to(root) else p.name
    fingerprint,size,mtime=file_fingerprint(p)
    con=connect_database(db_path);prev=_manifest_fingerprint(con,source_path);con.close()
    if prev and prev[0]==fingerprint and prev[1]=="complete" and prev[3]==PROCESSING_VERSION and limit_rows is None and not force:
        return {"skipped":True,"source_path":source_path,"status":"complete"}
    with p.open("rb") as f:
        return process_stream(f,source_path,db_path,file_fingerprint_value=fingerprint,file_size=size,source_class=_classify_source(source_path),limit_rows=limit_rows,force=force,output_location=output_location)

def process_zip_member(zip_path,member,db_path,*,root=None,limit_rows=None,force=False):
    zp=Path(zip_path);root=Path(root or zp.parent);rel=zp.relative_to(root).as_posix() if zp.is_relative_to(root) else zp.name
    source_path=f"{rel}!{member}"
    with zipfile.ZipFile(zp) as zf:
        info=zf.getinfo(member);stat=zp.stat()
        fingerprint=_zip_member_fingerprint(rel,stat,info)
        con=connect_database(db_path);prev=_manifest_fingerprint(con,source_path);con.close()
        if prev and prev[0]==fingerprint and prev[1]=="complete" and prev[3]==PROCESSING_VERSION and limit_rows is None and not force:
            return {"skipped":True,"source_path":source_path,"status":"complete"}
        with zf.open(info) as stream:
            return process_stream(stream,source_path,db_path,file_fingerprint_value=fingerprint,file_size=info.file_size,source_class=_classify_source(source_path),limit_rows=limit_rows,force=force,output_location=f"{rel}!{member}")

def discover_inputs(root):
    """Find raw CSVs and ZIPs while excluding app/build/derived directories."""
    root=Path(root).resolve();csvs=[];zips=[]
    for current,dirs,files in os.walk(root):
        dirs[:]=[d for d in dirs if d not in EXCLUDED_DIRS and not d.startswith(".")]
        for name in files:
            p=Path(current)/name
            if p.suffix.lower()==".csv" and name not in EXCLUDED_CSV_NAMES: csvs.append(p)
            elif p.suffix.lower()==".zip": zips.append(p)
    return sorted(csvs),sorted(zips)

def _mark_archive_copy(con,source_path,fingerprint,member,crc,size,content_sha256,matched_source):
    note=f"ZIP member {member} exactly matches {matched_source} by size, CRC32 and SHA-256; retained as a file-copy provenance record, not re-ingested."
    con.execute("INSERT INTO source_manifest(source_path,source_fingerprint,fingerprint_kind,content_sha256,file_crc32,source_class,processing_version,status,started_at,completed_at,input_records,valid_records,invalid_records,exact_duplicates,retained_records,new_canonical_records,potential_overlap_rows,notes) VALUES(?,?,?,?,?,'zip_member',?, 'copy_verified',current_timestamp,current_timestamp,0,0,0,0,0,0,0,?) ON CONFLICT(source_path) DO UPDATE SET status='copy_verified',completed_at=current_timestamp,content_sha256=excluded.content_sha256,file_crc32=excluded.file_crc32,notes=excluded.notes",[source_path,fingerprint,"zip_member_sha256",content_sha256,crc,PROCESSING_VERSION,note])

def _sha256_zip_member(zf,info):
    digest=hashlib.sha256()
    with zf.open(info) as stream:
        while chunk:=stream.read(1024*1024):digest.update(chunk)
    return digest.hexdigest()

def _zip_member_fingerprint(rel,stat,info):
    return hashlib.sha256(json.dumps([rel,stat.st_size,stat.st_mtime_ns,info.file_size,info.CRC],separators=(',',':')).encode()).hexdigest()

def process_all_inputs(root,db_path,*,limit_rows=None,force=False):
    root=Path(root).resolve();csvs,zips=discover_inputs(root);results=[]
    for p in csvs:
        results.append(process_csv(p,db_path,root=root,limit_rows=limit_rows,force=force))
    con=connect_database(db_path)
    basename_index=defaultdict(list)
    direct_sizes={}
    for p in csvs: direct_sizes.setdefault(p.name,[]).append((p.relative_to(root).as_posix(),p.stat().st_size))
    for row in con.execute("SELECT source_path,file_crc32,content_sha256 FROM source_manifest WHERE file_crc32 IS NOT NULL AND status='complete' AND content_sha256 IS NOT NULL").fetchall():
        basename_index[Path(row[0]).name].append((row[0],int(row[1]),row[2]))
    for zp in zips:
        rel=zp.relative_to(root).as_posix();stat=zp.stat()
        try: zf=zipfile.ZipFile(zp)
        except zipfile.BadZipFile: continue
        with zf:
            for info in zf.infolist():
                if info.is_dir() or not info.filename.lower().endswith(".csv"): continue
                member_path=f"{rel}!{info.filename}"
                if limit_rows is not None:
                    results.append({"source_path":member_path,"status":"not_processed_sample_run","reason":"ZIP members deferred during sampled runs"});continue
                matches=[m for m in basename_index.get(Path(info.filename).name,[]) if m[1]==info.CRC and any(d[0]==m[0] and d[1]==info.file_size for d in direct_sizes.get(Path(info.filename).name,[]))]
                if matches:
                    member_sha=_sha256_zip_member(zf,info)
                    if member_sha==matches[0][2]:
                        fp=_zip_member_fingerprint(rel,stat,info)
                        _mark_archive_copy(con,member_path,fp,info.filename,info.CRC,info.file_size,member_sha,matches[0][0])
                        results.append({"source_path":member_path,"status":"copy_verified","matched_source":matches[0][0]})
                    else:
                        results.append(process_zip_member(zp,info.filename,db_path,root=root,force=force))
                else:
                    results.append(process_zip_member(zp,info.filename,db_path,root=root,force=force))
    con.close()
    return results

def _normalize_legacy(row,source_path,source_fingerprint):
    dt_text=(row.get("datetime_utc") or "").strip(); stamp=None
    if dt_text:
        try: stamp=dt.datetime.fromisoformat(dt_text.replace("Z","+00:00")).astimezone(dt.timezone.utc)
        except ValueError: stamp=None
    acq_date=(row.get("date") or (stamp.date().isoformat() if stamp else None))
    acq_time=None
    if stamp: acq_time=stamp.strftime("%H%M")
    conf_raw=row.get("confidence") or None; conf_num=_num(conf_raw)
    cclass=(row.get("confidence_class") or ("numeric" if conf_num is not None else "unknown")).lower()
    cband=("low" if conf_num is not None and conf_num<30 else "nominal" if conf_num is not None and conf_num<80 else "high" if conf_num is not None else cclass)
    lat=_num(row.get("latitude"));lon=_num(row.get("longitude"));flags=["legacy_standardized_sample","original_raw_record_not_available_in_legacy_csv"]
    rec={"acquisition_datetime_utc":stamp.strftime("%Y-%m-%dT%H:%M:%SZ") if stamp else None,"acquisition_date":acq_date,
      "acquisition_time_original":acq_time,"original_acq_date":acq_date,"original_acq_time":acq_time,
      "latitude":lat,"longitude":lon,"sensor":row.get("sensor") or "UNKNOWN","satellite":row.get("satellite_std") or row.get("satellite") or "Unknown",
      "instrument":row.get("instrument") or None,"product_version":row.get("version") or None,"source_product":row.get("source_product") or "archive",
      "confidence_raw":conf_raw,"confidence_numeric":_num(row.get("confidence_numeric")) or conf_num,"confidence_class":cclass,"confidence_band":cband,
      "frp":_num(row.get("frp")),"brightness":_num(row.get("brightness")),"bright_t31":_num(row.get("bright_t31")),
      "bright_ti4":_num(row.get("bright_ti4")),"bright_ti5":_num(row.get("bright_ti5")),"scan":_num(row.get("scan")),"track":_num(row.get("track")),
      "daynight":(row.get("daynight") or "").upper() or None,"detection_type":row.get("detection_type") or None,
      "source_file":row.get("source_file") or Path(source_path).name,"source_path":source_path,"source_fingerprint":source_fingerprint,
      "raw_record_json":None,"quality_flags":_canonical_json(flags),"h3_cell":h3.latlng_to_cell(lat,lon,H3_RESOLUTION) if lat is not None and lon is not None else None,
      "provenance_level":"legacy_standardized_sample"}
    rec["row_fingerprint"]=exact_fingerprint(rec);rec["overlap_signature"]=overlap_signature(rec)
    return rec

def bootstrap_legacy_sample(root,db_path,legacy_path=None):
    """Seed the analytical warehouse from the known legacy sample without claiming full-source processing."""
    import gzip,itertools
    root=Path(root).resolve();legacy_path=Path(legacy_path or root/"outputs"/"observations.csv.gz");db_path=Path(db_path)
    if not legacy_path.is_file(): raise FileNotFoundError(f"Legacy processed sample not found: {legacy_path}")
    con=connect_database(db_path)
    current=con.execute("SELECT count(*) FROM observations").fetchone()[0]
    count=0;file_count=0
    with gzip.open(legacy_path,"rt",encoding="utf-8",newline="") as f:
        reader=csv.DictReader(f);seen=set()
        for source_file,group in itertools.groupby(reader,key=lambda r:r.get("source_file") or "unknown"):
            if source_file in seen: raise ValueError("Legacy sample rows are not grouped by source_file; aborting safe bootstrap")
            seen.add(source_file);source_path=source_file
            meta_path=root/source_file
            if meta_path.is_file():
                _,size,mtime=file_fingerprint(meta_path)
                source_fp=hashlib.sha256(f"legacy-sample\0{legacy_path.stat().st_size}\0{legacy_path.stat().st_mtime_ns}\0{source_file}\0{size}\0{mtime}".encode()).hexdigest()
            else:
                source_fp=hashlib.sha256(f"legacy-sample\0{legacy_path.stat().st_size}\0{legacy_path.stat().st_mtime_ns}\0{source_file}".encode()).hexdigest()
            existing=con.execute("SELECT status,fingerprint_kind,source_fingerprint,processing_version FROM source_manifest WHERE source_path=?",[source_path]).fetchone()
            if existing and (existing[1]!="legacy_output_lineage_not_raw_checksum" or (existing[0]=="sampled" and existing[2]==source_fp and existing[3]==PROCESSING_VERSION)):
                file_count+=1
                continue
            items=list(group)
            records=[_normalize_legacy(row,source_path,source_fp) for row in items]
            result=merge_records(con,source_path,source_fp,records,source_class=_classify_source(source_path),sample_limit=20000,
              fingerprint_kind="legacy_output_lineage_not_raw_checksum",output_location=str(legacy_path),missing={"legacy_raw_fields_unavailable":len(items)})
            count+=result.get("new_canonical_records",0);file_count+=1
    con.close()
    return {"status":"bootstrapped_legacy_sample","records_added":count,"source_files":file_count,"legacy_sample":str(legacy_path)}

def assign_countries(con,boundary_path):
    """Assign previously unassigned points to local bundled country polygons; no network access."""
    boundary_path=Path(boundary_path)
    if not boundary_path.is_file(): return {"status":"boundary_file_missing","assigned":0}
    import shapely
    import numpy as np
    from shapely.geometry import shape
    from shapely.strtree import STRtree
    data=json.loads(boundary_path.read_text(encoding="utf-8"));geoms=[];attrs=[]
    for feature in data.get("features",[]):
        try:
            geom=shape(feature["geometry"]);props=feature.get("properties") or {}
            geoms.append(geom);attrs.append((str(feature.get("id") or props.get("iso_n3") or props.get("ISO_N3") or ""),str(props.get("name") or props.get("NAME") or "Unknown")))
        except (KeyError,TypeError,ValueError): continue
    if not geoms: return {"status":"no_country_geometries","assigned":0}
    tree=STRtree(geoms);assigned=0;cursor=con.cursor()
    cursor.execute("SELECT o.row_fingerprint,o.longitude,o.latitude FROM observations o LEFT JOIN country_assignment c USING(row_fingerprint) WHERE c.row_fingerprint IS NULL")
    with tempfile.TemporaryDirectory(prefix="firescope-country-") as tmp:
        stage_csv=Path(tmp)/"countries.csv"
        with stage_csv.open("w",newline="",encoding="utf-8") as sf:
            writer=csv.writer(sf);writer.writerow(["row_fingerprint","country_id","country_name","boundary_version"])
            while rows:=cursor.fetchmany(20000):
                points=shapely.points([r[1] for r in rows],[r[2] for r in rows])
                hits=tree.query(points,predicate="covered_by")
                chosen={}
                if hits.size:
                    order=np.lexsort((hits[1],hits[0]));indices=hits[0,order];first=np.r_[True,indices[1:]!=indices[:-1]]
                    chosen=dict(zip(indices[first].tolist(),hits[1,order][first].tolist()))
                for ix,(fp,_,_) in enumerate(rows):
                    code,name=attrs[chosen[ix]] if ix in chosen else ("","Unassigned / international waters")
                    writer.writerow([fp,code,name,"world-atlas Natural Earth 50m"])
                assigned+=len(rows)
        if assigned:
            con.execute("INSERT INTO country_assignment(row_fingerprint,country_id,country_name,boundary_version) SELECT row_fingerprint,coalesce(country_id,''),coalesce(country_name,'Unassigned / international waters'),coalesce(boundary_version,'world-atlas Natural Earth 50m') FROM read_csv_auto(?,header=true,all_varchar=true,nullstr='') ON CONFLICT(row_fingerprint) DO NOTHING",[str(stage_csv)])
    return {"status":"ok","assigned":assigned,"boundary_features":len(geoms),"boundary_version":"world-atlas Natural Earth 50m"}

def _sql_literal(value): return "'"+str(value).replace("'","''").replace("\\","\\\\")+"'"

def _copy_parquet(con,query,target):
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True);tmp=target.with_name(target.name+".tmp")
    if tmp.exists(): tmp.unlink()
    con.execute(f"COPY ({query}) TO {_sql_literal(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")
    os.replace(tmp,target)
    return target.stat().st_size

def _copy_csv(con,query,target):
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True);tmp=target.with_name(target.name+".tmp")
    if tmp.exists(): tmp.unlink()
    con.execute(f"COPY ({query}) TO {_sql_literal(tmp)} (FORMAT CSV, HEADER TRUE, DELIMITER ',')")
    os.replace(tmp,target);return target.stat().st_size

def finalize_warehouse(db_path,root,*,boundary_path=None,export=True):
    """Rebuild reproducible DuckDB aggregates from canonical observations and refresh changed Parquet partitions."""
    root=Path(root).resolve();con=connect_database(db_path)
    country_result=assign_countries(con,boundary_path) if boundary_path else {"status":"not_configured","assigned":0}
    con.execute("CREATE OR REPLACE TABLE daily_activity AS SELECT acquisition_date AS date,sensor,satellite,product_version,confidence_band,daynight,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw,count(*) FILTER(WHERE frp IS NULL) AS missing_frp FROM observations GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE monthly_activity AS SELECT date_trunc('month',acquisition_date)::DATE AS month,sensor,satellite,product_version,confidence_band,daynight,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw,count(*) FILTER(WHERE frp IS NULL) AS missing_frp FROM observations GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE annual_activity AS SELECT date_trunc('year',acquisition_date)::DATE AS year,sensor,satellite,product_version,confidence_band,daynight,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw,count(*) FILTER(WHERE frp IS NULL) AS missing_frp FROM observations GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE h3_daily AS SELECT acquisition_date AS date,h3_cell,6 AS h3_resolution,sensor,satellite,product_version,confidence_band,daynight,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw FROM observations WHERE h3_cell IS NOT NULL GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE grid2_daily AS SELECT acquisition_date AS date,floor((longitude+180)/2)::INTEGER AS grid_x,floor((latitude+90)/2)::INTEGER AS grid_y,sensor,satellite,product_version,confidence_band,daynight,count(*) AS detection_count,sum(frp) AS frp_sum_mw FROM observations GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE coverage_report AS SELECT acquisition_date AS date,sensor,satellite,product_version,h3_cell,count(*) AS detections,'detections_observed; acquisition opportunity unknown' AS coverage_status FROM observations GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE country_daily AS SELECT c.country_id,c.country_name,o.acquisition_date AS date,o.sensor,o.satellite,o.product_version,o.confidence_band,o.daynight,count(*) AS detection_count,sum(o.frp) AS frp_sum_mw,avg(o.frp) AS frp_mean_mw,median(o.frp) AS frp_median_mw,count(*) FILTER(WHERE o.frp IS NULL) AS missing_frp FROM observations o JOIN country_assignment c USING(row_fingerprint) WHERE c.country_id<>'' GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE country_grid2_daily AS SELECT c.country_name,o.acquisition_date AS date,floor((o.longitude+180)/2)::INTEGER AS grid_x,floor((o.latitude+90)/2)::INTEGER AS grid_y,o.sensor,o.satellite,o.product_version,o.confidence_band,o.daynight,count(*) AS detection_count FROM observations o JOIN country_assignment c USING(row_fingerprint) WHERE c.country_id<>'' GROUP BY ALL")
    con.execute("CREATE OR REPLACE TABLE sensor_comparison AS SELECT date,h3_cell,sum(detection_count) FILTER(WHERE sensor='MODIS') AS modis_count,sum(detection_count) FILTER(WHERE sensor='VIIRS') AS viirs_count,sum(frp_sum_mw) FILTER(WHERE sensor='MODIS') AS modis_frp_sum_mw,sum(frp_sum_mw) FILTER(WHERE sensor='VIIRS') AS viirs_frp_sum_mw FROM h3_daily GROUP BY date,h3_cell HAVING coalesce(sum(detection_count) FILTER(WHERE sensor='MODIS'),0)>0 AND coalesce(sum(detection_count) FILTER(WHERE sensor='VIIRS'),0)>0")
    con.execute("CREATE OR REPLACE TABLE potential_overlap_groups AS SELECT overlap_signature,count(DISTINCT row_fingerprint) AS observation_count,any_value(sensor) AS sensor,any_value(satellite) AS satellite,min(acquisition_date) AS date_min,max(acquisition_date) AS date_max,string_agg(DISTINCT coalesce(product_version,''),',') AS versions,string_agg(DISTINCT source_path,',') AS sources,current_timestamp AS updated_at FROM observations GROUP BY overlap_signature HAVING count(DISTINCT row_fingerprint)>1")
    con.execute("CREATE OR REPLACE TABLE sensor_activity AS SELECT sensor,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw,count(*) FILTER(WHERE frp IS NULL) AS missing_frp FROM observations GROUP BY sensor")
    con.execute("CREATE OR REPLACE TABLE satellite_activity AS SELECT satellite,sensor,count(*) AS detection_count,sum(frp) AS frp_sum_mw,avg(frp) AS frp_mean_mw,median(frp) AS frp_median_mw FROM observations GROUP BY satellite,sensor")
    con.execute("CREATE OR REPLACE TABLE harmonization_metadata AS SELECT 'H3 resolution 6; UTC acquisition date; native sensor rows retained' AS comparison_basis,'descriptive_comparison_only; no validated calibration model' AS model_status,current_timestamp AS refreshed_at")
    if export:
        parquet_root=root/"data"/"processed"/"parquet";agg_root=root/"data"/"processed"/"aggregates"
        years=[r[0] for r in con.execute("SELECT DISTINCT year(acquisition_date) FROM observations WHERE acquisition_date IS NOT NULL ORDER BY 1").fetchall()]
        for year in years:
            _copy_parquet(con,f"SELECT * FROM observations WHERE year(acquisition_date)={int(year)} ORDER BY acquisition_date,sensor,satellite",parquet_root/f"year={int(year)}"/"observations.parquet")
        for table in ("daily_activity","monthly_activity","annual_activity","h3_daily","grid2_daily","country_daily","country_grid2_daily","coverage_report","sensor_comparison","sensor_activity","satellite_activity"):
            _copy_parquet(con,f"SELECT * FROM {table}",agg_root/f"{table}.parquet")
        _copy_csv(con,"SELECT * FROM coverage_report ORDER BY date,sensor,satellite,h3_cell",root/"COVERAGE_REPORT.csv")
        _copy_csv(con,"SELECT * FROM source_manifest ORDER BY source_path",root/"PROCESSING_MANIFEST.csv")
        _copy_csv(con,"SELECT * FROM duplicate_records ORDER BY source_path,row_fingerprint",root/"DUPLICATE_RECORDS.csv")
        _copy_csv(con,"SELECT * FROM sensor_comparison ORDER BY date,h3_cell",root/"HARMONIZATION_COMPARISON.csv")
    duplicate_counts=duplicate_accounting(con)
    result={"observations":con.execute("SELECT count(*) FROM observations").fetchone()[0],**duplicate_counts,"potential_overlap_groups":con.execute("SELECT count(*) FROM potential_overlap_groups").fetchone()[0],"coverage_rows":con.execute("SELECT count(*) FROM coverage_report").fetchone()[0],"sensor_comparison_rows":con.execute("SELECT count(*) FROM sensor_comparison").fetchone()[0],"country_assignments":con.execute("SELECT count(*) FROM country_assignment").fetchone()[0],"country_assignment_status":country_result}
    con.close();return result

def main():
    import argparse
    parser=argparse.ArgumentParser(description="Incremental FIRMS point-observation warehouse; no acquisition or API access")
    parser.add_argument("command",choices=("init","ingest","rebuild-aggregates","status"))
    parser.add_argument("--root",default=".");parser.add_argument("--db",default="warehouse/firescope.duckdb")
    parser.add_argument("--boundary-file",default="data/reference/countries_50m.geojson")
    parser.add_argument("--limit-rows-per-file",type=int,default=None);parser.add_argument("--force",action="store_true")
    args=parser.parse_args();root=Path(args.root).resolve();db=Path(args.db);db=db if db.is_absolute() else root/db
    boundary=Path(args.boundary_file);boundary=boundary if boundary.is_absolute() else root/boundary
    if args.command=="init":
        result=bootstrap_legacy_sample(root,db)
        result["warehouse"]=finalize_warehouse(db,root,boundary_path=boundary if boundary.is_file() else None)
    elif args.command=="ingest":
        files=process_all_inputs(root,db,limit_rows=args.limit_rows_per_file,force=args.force)
        result={"sources_seen":len(files),"source_status_counts":dict(Counter(x.get("status","unknown") for x in files)),"warehouse":finalize_warehouse(db,root,boundary_path=boundary if boundary.is_file() else None)}
    elif args.command=="rebuild-aggregates":
        result=finalize_warehouse(db,root,boundary_path=boundary if boundary.is_file() else None)
    else:
        con=connect_database(db)
        result={"observations":con.execute("SELECT count(*) FROM observations").fetchone()[0],"manifest_status":dict(con.execute("SELECT status,count(*) FROM source_manifest GROUP BY status").fetchall()),"invalid_records":con.execute("SELECT count(*) FROM invalid_observations").fetchone()[0],**duplicate_accounting(con)}
        con.close()
    print(json.dumps(result,indent=2,default=str))

if __name__=="__main__": main()
