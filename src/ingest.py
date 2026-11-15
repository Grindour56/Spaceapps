#!/usr/bin/env python3
"""Streaming FIRMS point-observation normalizer; never mutates source data."""
import argparse, csv, datetime as dt, gzip, hashlib, json, logging, math, os, random, re
from collections import Counter, defaultdict
from pathlib import Path

FIELDS = ['datetime_utc','date','latitude','longitude','sensor','satellite_std','instrument','confidence','confidence_class','frp','daynight','detection_type','brightness','bright_t31','bright_ti4','bright_ti5','scan','track','version','source_file','source_product']

def num(x):
    try:
        v=float(x); return v if math.isfinite(v) else None
    except (TypeError,ValueError): return None

def normalize_row(r, source):
    r={str(k).strip().lower():v for k,v in r.items() if k is not None}
    lat=num(r.get('latitude',r.get('lat'))); lon=num(r.get('longitude',r.get('lon')))
    if lat is None or lon is None or not (-90<=lat<=90 and -180<=lon<=180): return None
    date=str(r.get('acq_date',r.get('date',''))).strip()
    time=str(r.get('acq_time',r.get('time','0000'))).strip().zfill(4)
    try: stamp=dt.datetime.strptime(date+time[:4],'%Y-%m-%d%H%M').replace(tzinfo=dt.timezone.utc)
    except ValueError: return None
    instrument=str(r.get('instrument','')).upper()
    satellite=str(r.get('satellite','')).strip()
    fname=Path(source).name
    if not instrument: instrument='VIIRS' if any(x in (fname+' '+satellite).upper() for x in ('SV-C2','J1V','J2V','SNPP','N20','N21')) else ('MODIS' if 'M-C' in fname.upper() or satellite.lower() in ('terra','aqua') else 'UNKNOWN')
    if instrument in ('SNPP','NPP','VIIRS'): instrument='VIIRS'
    satmap={'TERRA':'Terra','AQUA':'Aqua','SNPP':'Suomi-NPP','NPP':'Suomi-NPP','N20':'NOAA-20','NOAA20':'NOAA-20','J1V':'NOAA-20','N21':'NOAA-21','NOAA21':'NOAA-21','J2V':'NOAA-21'}
    sat=satmap.get(satellite.upper(), satellite or 'Unknown')
    conf=str(r.get('confidence','')).strip().lower()
    cclass=('low' if conf in ('l','low') else 'nominal' if conf in ('n','nominal') else 'high' if conf in ('h','high') else 'numeric' if num(conf) is not None else 'unknown')
    get=lambda k:num(r.get(k))
    product='NRT' if 'fire_nrt' in fname.lower() or 'nrt' in str(r.get('version','')).lower() else 'archive'
    return {'datetime_utc':stamp.strftime('%Y-%m-%dT%H:%M:%SZ'),'date':stamp.date().isoformat(),'latitude':lat,'longitude':lon,'sensor':instrument,'satellite_std':sat,'instrument':r.get('instrument',''),'confidence':conf,'confidence_class':cclass,'frp':get('frp'),'daynight':str(r.get('daynight','')).upper() or None,'detection_type':r.get('type'),'brightness':get('brightness'),'bright_t31':get('bright_t31'),'bright_ti4':get('bright_ti4'),'bright_ti5':get('bright_ti5'),'scan':get('scan'),'track':get('track'),'version':r.get('version'),'source_file':fname,'source_product':product}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--input',default='.'); ap.add_argument('--output',default='outputs'); ap.add_argument('--max-files',type=int,default=0); ap.add_argument('--max-rows',type=int,default=0); ap.add_argument('--rows-per-file',type=int,default=0); ap.add_argument('--force-output',action='store_true',help='Explicitly replace existing legacy demo outputs'); a=ap.parse_args()
    root=Path(a.input).resolve(); out=Path(a.output).resolve(); out.mkdir(parents=True,exist_ok=True)
    targets=[out/'observations.csv.gz',out/'daily_activity.csv',out/'map_sample.json',out/'quality_report.json']
    existing=[str(p) for p in targets if p.exists()]
    if existing and not a.force_output: ap.error('Refusing to overwrite existing derived outputs; pass --force-output only when you intend to replace them.')
    files=sorted(p for p in root.glob('*.csv') if p.name.startswith('fire_archive_'))
    # Do not ingest outputs or country exports; only top-level finalized archive inputs.
    if a.max_files: files=files[:a.max_files]
    stats={'files_seen':len(files),'files_failed':[],'raw_rows':0,'valid_rows':0,'invalid_rows':0,'exact_duplicates':None,'missing':Counter(),'sensors':Counter(),'satellites':Counter(),'date_min':None,'date_max':None,'geo':{'lat_min':90,'lat_max':-90,'lon_min':180,'lon_max':-180}}
    daily=defaultdict(lambda:[0,0.0,0,0]); sample=[]; rng=random.Random(42); sample_cap=12000
    target=out/'observations.csv.gz'
    with gzip.open(target,'wt',newline='',encoding='utf-8') as gz:
      w=csv.DictWriter(gz,fieldnames=FIELDS); w.writeheader()
      for file in files:
       if a.max_rows and stats['raw_rows']>=a.max_rows: break
       try:
        file_rows=0
        with file.open('r',encoding='utf-8-sig',newline='') as f:
         reader=csv.DictReader(f)
         for row in reader:
          if a.max_rows and stats['raw_rows']>=a.max_rows: break
          if a.rows_per_file and file_rows>=a.rows_per_file: break
          file_rows+=1; stats['raw_rows']+=1; o=normalize_row(row,file.name)
          if o is None: stats['invalid_rows']+=1; continue
          # Identical ZIP copies and overlapping NRT/country exports are excluded at source selection.
          w.writerow(o); stats['valid_rows']+=1; stats['sensors'][o['sensor']]+=1; stats['satellites'][o['satellite_std']]+=1
          stats['date_min']=o['date'] if stats['date_min'] is None else min(stats['date_min'],o['date']); stats['date_max']=o['date'] if stats['date_max'] is None else max(stats['date_max'],o['date'])
          g=stats['geo']; g['lat_min']=min(g['lat_min'],o['latitude']); g['lat_max']=max(g['lat_max'],o['latitude']); g['lon_min']=min(g['lon_min'],o['longitude']); g['lon_max']=max(g['lon_max'],o['longitude'])
          d=daily[o['date']]; d[0]+=1; d[1]+=o['frp'] or 0; d[2]+=o['sensor']=='MODIS'; d[3]+=o['sensor']=='VIIRS'
          for col in ('frp','confidence','brightness','bright_t31','daynight'):
           if o.get(col) in (None,''): stats['missing'][col]+=1
          if len(sample)<sample_cap: sample.append(o)
          else:
           j=rng.randrange(stats['valid_rows'])
           if j<sample_cap: sample[j]=o
        logging.info('processed %s',file.name)
       except Exception as e: stats['files_failed'].append({'file':file.name,'error':str(e)[:300]}); logging.exception('failed %s',file.name)
    with (out/'daily_activity.csv').open('w',newline='') as f:
      w=csv.writer(f); w.writerow(['date','hotspot_count','frp_sum_mw','modis_count','viirs_count'])
      for day,vals in sorted(daily.items()): w.writerow([day,*[round(x,3) if i==1 else x for i,x in enumerate(vals)]])
    (out/'map_sample.json').write_text(json.dumps(sample,separators=(',',':')))
    stats['sensors']=dict(stats['sensors']); stats['satellites']=dict(stats['satellites']); stats['missing']=dict(stats['missing'])
    stats['files_processed']=len(files)-len(stats['files_failed']); stats['run_sampled']=bool((a.max_rows and stats['raw_rows']>=a.max_rows) or a.rows_per_file or a.max_files); stats['partial']=True; stats['input_coverage_complete']=False; stats['deduplication']='Source-file overlaps excluded by policy; exact row-level duplicate audit not performed in this MVP.'; stats['source_policy']='Top-level finalized archive CSV only; NRT, ZIP copies, and country-year exports excluded to prevent unverified overlap.'
    (out/'quality_report.json').write_text(json.dumps(stats,indent=2))
    print(json.dumps({k:v for k,v in stats.items() if k not in ('missing','geo')},indent=2)); print('Wrote',target)
if __name__=='__main__': main()
