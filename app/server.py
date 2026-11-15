#!/usr/bin/env python3
"""Allowlisted local dashboard server plus exact filtered summaries over processed rows."""
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
import csv, gzip, json, mimetypes, threading, sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / "src"))
from analytics import aggregate_rows
from warehouse_queries import query_warehouse, get_coverage_summary, get_sensor_comparison

OUTPUTS = ROOT / "outputs"
WAREHOUSE = ROOT / "warehouse" / "firescope.duckdb"
def _vendor_file(node_rel, vendor_name):
    p = ROOT / "app" / "node_modules" / node_rel
    return p if p.is_file() else ROOT / "app" / "vendor" / vendor_name

ALLOWED = {
    "/": ROOT / "app" / "index.html",
    "/index.html": ROOT / "app" / "index.html",
    "/outputs/quality_report.json": OUTPUTS / "quality_report.json",
    "/vendor/leaflet.js": _vendor_file("leaflet/dist/leaflet.js", "leaflet.js"),
    "/vendor/leaflet.css": _vendor_file("leaflet/dist/leaflet.css", "leaflet.css"),
    "/vendor/topojson-client.js": _vendor_file("topojson-client/dist/topojson-client.min.js", "topojson-client.js"),
    "/vendor/countries-50m.json": _vendor_file("world-atlas/countries-50m.json", "countries-50m.json"),
}
CACHE = OrderedDict()
CACHE_LOCK = threading.Lock()
CACHE_MAX = 4

def _read_observations():
    path=OUTPUTS / "observations.csv.gz"
    if not path.is_file(): raise FileNotFoundError("Run src/ingest.py to create processed observations")
    with gzip.open(path,"rt",encoding="utf-8",newline="") as f:
        for row in csv.DictReader(f):
            for key in ("latitude","longitude","frp","brightness","bright_t31","bright_ti4","bright_ti5"):
                value=row.get(key)
                if value in (None,""): row[key]=None
                else:
                    try: row[key]=float(value)
                    except (TypeError,ValueError): row[key]=None
            yield row

def _query(payload):
    source=WAREHOUSE if WAREHOUSE.is_file() else OUTPUTS/"observations.csv.gz"
    stamp=(source.stat().st_size,source.stat().st_mtime_ns) if source.exists() else (0,0)
    key=json.dumps([stamp,payload],sort_keys=True,separators=(",",":"))
    with CACHE_LOCK:
        hit=CACHE.get(key)
        if hit is not None:
            CACHE.move_to_end(key)
            return hit
    if WAREHOUSE.is_file():
        result=query_warehouse(WAREHOUSE,payload)
    else:
        result=aggregate_rows(_read_observations(),payload)
        try:
            quality=json.loads((OUTPUTS/"quality_report.json").read_text())
            result.update({"partial":quality.get("partial",True),"input_coverage_complete":quality.get("input_coverage_complete",False),"run_sampled":quality.get("run_sampled",False),"date_min":quality.get("date_min"),"date_max":quality.get("date_max"),"source_records":quality.get("valid_rows",0)})
        except Exception: result.update({"partial":True,"date_min":None,"date_max":None,"source_records":0})
    with CACHE_LOCK:
        CACHE[key]=result
        CACHE.move_to_end(key)
        while len(CACHE)>CACHE_MAX: CACHE.popitem(last=False)
    return result

def _valid_iso_date(value):
    if not value: return ""
    import datetime as dt
    try: return dt.date.fromisoformat(value).isoformat()
    except ValueError: raise ValueError("Dates must use YYYY-MM-DD")

def _coordinate_count(value):
    if isinstance(value,(list,tuple)):
        if len(value)>=2 and isinstance(value[0],(int,float)) and isinstance(value[1],(int,float)): return 1
        return sum(_coordinate_count(x) for x in value)
    return 0

def _health_payload():
    if WAREHOUSE.is_file():
        try:
            import duckdb
            with duckdb.connect(str(WAREHOUSE),read_only=True) as con:
                records=con.execute("SELECT count(*) FROM observations").fetchone()[0]
                lo,hi=con.execute("SELECT min(acquisition_date),max(acquisition_date) FROM observations").fetchone()
                sampled=con.execute("SELECT count(*) FROM source_manifest WHERE status='sampled'").fetchone()[0]
            return {"ok":True,"partial":True,"run_sampled":sampled>0,"input_coverage_complete":False,"records":records,"date_min":str(lo) if lo else None,"date_max":str(hi) if hi else None}
        except Exception: pass
    report=ALLOWED["/outputs/quality_report.json"]
    try: q=json.loads(report.read_text())
    except Exception: q={}
    return {"ok":True,"partial":q.get("partial",True),"run_sampled":q.get("run_sampled",False),"input_coverage_complete":q.get("input_coverage_complete",False),"records":q.get("valid_rows",0),"date_min":q.get("date_min"),"date_max":q.get("date_max")}

def _analytics_response(raw_body):
    if not raw_body or len(raw_body)>2_000_000:
        return 413,{"error":"Request body must be between 1 byte and 2 MB"}
    try:
        payload=json.loads(raw_body)
        if not isinstance(payload,dict): raise ValueError("Expected a JSON object")
        if payload.get("sensor","all") not in ("all","MODIS","VIIRS"): raise ValueError("Invalid sensor filter")
        if payload.get("confidence","all") not in ("all","low","nominal","high"): raise ValueError("Invalid confidence filter")
        if "satellites" in payload and (not isinstance(payload["satellites"],list) or not set(payload["satellites"]).issubset({"Terra","Aqua","Suomi-NPP","NOAA-20","NOAA-21"})): raise ValueError("Invalid satellite selection")
        payload["start"]=_valid_iso_date(payload.get("start")); payload["end"]=_valid_iso_date(payload.get("end")); payload["selected_date"]=_valid_iso_date(payload.get("selected_date"))
        if payload["start"] and payload["end"] and payload["start"]>payload["end"]: raise ValueError("Start date must not follow end date")
        if payload.get("geometry") is not None:
            if not isinstance(payload["geometry"],dict) or payload["geometry"].get("type") not in ("Polygon","MultiPolygon"): raise ValueError("AOI must be a GeoJSON Polygon or MultiPolygon")
            if _coordinate_count(payload["geometry"].get("coordinates"))>50000: raise ValueError("AOI exceeds 50,000 vertices")
        if payload.get("country") is not None and (not isinstance(payload["country"],str) or len(payload["country"])>120): raise ValueError("Invalid country filter")
        return 200,_query(payload)
    except FileNotFoundError as e: return 503,{"error":str(e)}
    except (json.JSONDecodeError,ValueError,TypeError,KeyError) as e: return 400,{"error":str(e)}
    except Exception as e: print("analytics request failed:",type(e).__name__,str(e)[:200]); return 500,{"error":"Analytics query failed"}

class Handler(BaseHTTPRequestHandler):
    def _send_json(self,status,obj):
        data=json.dumps(obj,separators=(",",":"),allow_nan=False).encode()
        self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        path=urlsplit(self.path).path
        if path=="/api/health":
            self._send_json(200,_health_payload()); return
        if path=="/api/coverage" and WAREHOUSE.is_file():
            self._send_json(200,get_coverage_summary(WAREHOUSE)); return
        if path=="/api/sensor-comparison" and WAREHOUSE.is_file():
            self._send_json(200,get_sensor_comparison(WAREHOUSE)); return
        target=ALLOWED.get(path)
        if target is None or not target.is_file(): self.send_error(404,"Not found"); return
        content_type=mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_response(200); self.send_header("Content-Type",content_type); self.send_header("Content-Length",str(target.stat().st_size)); self.send_header("X-Content-Type-Options","nosniff"); self.end_headers()
        with target.open("rb") as f:
            while chunk:=f.read(1024*1024): self.wfile.write(chunk)
    def do_POST(self):
        if urlsplit(self.path).path!="/api/analytics": self.send_error(404,"Not found"); return
        try: length=int(self.headers.get("Content-Length","0"))
        except ValueError: length=0
        if length<=0 or length>2_000_000: self._send_json(413,{"error":"Request body must be between 1 byte and 2 MB"}); return
        status,obj=_analytics_response(self.rfile.read(length))
        self._send_json(status,obj)
    def log_message(self,fmt,*args): print(fmt%args)

class handler(Handler):
    pass

STATUS_TEXT={200:"200 OK",400:"400 Bad Request",404:"404 Not Found",405:"405 Method Not Allowed",413:"413 Payload Too Large",500:"500 Internal Server Error",503:"503 Service Unavailable"}

def app(environ,start_response):
    method=(environ.get("REQUEST_METHOD") or "GET").upper()
    path=urlsplit(environ.get("PATH_INFO") or "/").path or "/"
    def json_reply(code,obj):
        data=json.dumps(obj,separators=(",",":"),allow_nan=False).encode()
        start_response(STATUS_TEXT.get(code,f"{code} OK"),[("Content-Type","application/json; charset=utf-8"),("Content-Length",str(len(data))),("Cache-Control","no-store")])
        return [data]
    if method=="GET":
        if path=="/api/health": return json_reply(200,_health_payload())
        if path=="/api/coverage" and WAREHOUSE.is_file(): return json_reply(200,get_coverage_summary(WAREHOUSE))
        if path=="/api/sensor-comparison" and WAREHOUSE.is_file(): return json_reply(200,get_sensor_comparison(WAREHOUSE))
        target=ALLOWED.get(path)
        if target is None or not target.is_file(): return json_reply(404,{"error":"Not found"})
        content_type=mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data=target.read_bytes()
        start_response("200 OK",[("Content-Type",content_type),("Content-Length",str(len(data))),("X-Content-Type-Options","nosniff")])
        return [data]
    if method=="POST":
        if path!="/api/analytics": return json_reply(404,{"error":"Not found"})
        try: length=int(environ.get("CONTENT_LENGTH") or "0")
        except ValueError: length=0
        if length<=0 or length>2_000_000: return json_reply(413,{"error":"Request body must be between 1 byte and 2 MB"})
        stream=environ.get("wsgi.input")
        raw_body=stream.read(length) if stream else b""
        code,obj=_analytics_response(raw_body)
        return json_reply(code,obj)
    return json_reply(405,{"error":"Method not allowed"})

application = app

if __name__=="__main__":
    print("FireScope dashboard: http://127.0.0.1:8000")
    ThreadingHTTPServer(("127.0.0.1",8000),Handler).serve_forever()
