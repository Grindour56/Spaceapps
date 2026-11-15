"""Sensor-aware aggregations over normalized observation records."""
from collections import defaultdict
import math, random
from spatial import prepare_geometry, prepared_contains

MAP_FIELDS=('datetime_utc','date','latitude','longitude','sensor','satellite_std','instrument','confidence','frp','brightness','bright_t31','bright_ti4','bright_ti5','daynight','detection_type','version','source_file')

def _numeric_confidence(value):
    try: return float(value)
    except (TypeError,ValueError): return None

def _passes_confidence(row, level):
    if level in (None,'','all'): return True
    c=str(row.get('confidence_class','')).lower(); n=_numeric_confidence(row.get('confidence'))
    if level=='high': return c=='high' or (n is not None and n>=80)
    if level=='nominal': return c in ('high','nominal') or (n is not None and n>=30)
    if level=='low': return c=='low' or (n is not None and n<30)
    return True

def _bounds(geometry):
    if not geometry: return None
    positions=[]
    def walk(node):
        if isinstance(node,(list,tuple)):
            if len(node)>=2 and isinstance(node[0],(int,float)) and isinstance(node[1],(int,float)): positions.append((node[0],node[1]))
            else:
                for child in node: walk(child)
    walk(geometry.get('coordinates',[]))
    if not positions: return None
    return min(p[0] for p in positions),min(p[1] for p in positions),max(p[0] for p in positions),max(p[1] for p in positions)

def _empty_bucket(day):
    return {'date':day,'count':0,'frp_sum_mw':0.0,'missing_frp':0,'sensors':defaultdict(int),'satellites':defaultdict(int),'daynight':defaultdict(int)}

def _serial_bucket(b):
    return {'date':b['date'],'count':b['count'],'frp_sum_mw':round(b['frp_sum_mw'],3),'missing_frp':b['missing_frp'],'sensors':dict(b['sensors']),'satellites':dict(b['satellites']),'daynight':dict(b['daynight'])}

def aggregate_rows(rows, filters=None, map_limit=2000, grid_limit=5000):
    """Aggregate every qualifying row; map_points is a separate bounded reservoir sample."""
    f=filters or {}; start=f.get('start') or ''; end=f.get('end') or ''; selected=f.get('selected_date') or ''
    sensor=f.get('sensor') or 'all'; satellites=set(f['satellites']) if 'satellites' in f else None; confidence=f.get('confidence') or 'all'; geometry=f.get('geometry')
    bounds=_bounds(geometry); prepared=prepare_geometry(geometry); by_day={}; by_month={}; by_year={}; sensors=defaultdict(int); sats=defaultdict(int); daynight=defaultdict(int); confidence_counts=defaultdict(int); grid=defaultdict(int)
    total=0; frp_total=0.0; missing_frp=0; map_seen=0; map_points=[]; rng=random.Random(4815162342)
    for row in rows:
        day=str(row.get('date') or '')
        if not day or (start and day<start) or (end and day>end): continue
        if sensor!='all' and row.get('sensor')!=sensor: continue
        if satellites is not None and row.get('satellite_std') not in satellites: continue
        if not _passes_confidence(row,confidence): continue
        lat=row.get('latitude'); lon=row.get('longitude')
        if geometry:
            if bounds and not (bounds[0]-1e-9<=lon<=bounds[2]+1e-9 and bounds[1]-1e-9<=lat<=bounds[3]+1e-9):
                # Antimeridian-crossing polygons are not rejected by their world-spanning bbox.
                coords=geometry.get('coordinates',[])
                if bounds[2]-bounds[0]<180: continue
            if not prepared_contains(lon,lat,prepared,geometry): continue
        total+=1; value=row.get('frp')
        if value in (None,''): missing_frp+=1; frp=0.0; frp_missing=True
        else:
            try: frp=float(value); frp_missing=False
            except (TypeError,ValueError): frp=0.0; missing_frp+=1; frp_missing=True
        frp_total+=frp
        sat=row.get('satellite_std') or 'Unknown'; sen=row.get('sensor') or 'Unknown'; dn=row.get('daynight') or 'Unknown'; conf=str(row.get('confidence_class') or 'unknown')
        sensors[sen]+=1; sats[sat]+=1; daynight[dn]+=1; confidence_counts[conf]+=1
        month=day[:7]; year=day[:4]
        for table,key in ((by_day,day),(by_month,month),(by_year,year)):
            b=table.setdefault(key,_empty_bucket(key)); b['count']+=1; b['frp_sum_mw']+=frp; b['missing_frp']+=frp_missing; b['sensors'][sen]+=1; b['satellites'][sat]+=1; b['daynight'][dn]+=1
        if not selected or day==selected:
            gx=math.floor((lon+180)/2); gy=math.floor((lat+90)/2); grid[(gx,gy)]+=1
            map_seen+=1
            if len(map_points)<map_limit: map_points.append({k:row.get(k) for k in MAP_FIELDS})
            else:
                ix=rng.randrange(map_seen)
                if ix<map_limit: map_points[ix]={k:row.get(k) for k in MAP_FIELDS}
    daily=[_serial_bucket(by_day[k]) for k in sorted(by_day)]
    monthly=[_serial_bucket(by_month[k]) for k in sorted(by_month)]
    yearly=[_serial_bucket(by_year[k]) for k in sorted(by_year)]
    dens=sorted(grid.items(),key=lambda x:(-x[1],x[0]))[:grid_limit]
    return {'total':total,'frp_sum_mw':round(frp_total,3),'missing_frp':missing_frp,'sensors':dict(sensors),'satellites':dict(sats),'daynight':dict(daynight),'confidence_classes':dict(confidence_counts),'daily':daily,'monthly':monthly,'yearly':yearly,'peak_day':max(daily,key=lambda x:x['count'])['date'] if daily else None,'map_points':map_points,'map_points_total':map_seen,'map_grid':[{'latitude':gy*2-89,'longitude':gx*2-179,'count':n} for (gx,gy),n in dens],'grid_cells_total':len(grid),'coverage_note':'Dates with detections are shown. Absence of a row is not confirmed zero activity; sensor acquisition coverage is unavailable. The current source files may be a partial extract.'}
