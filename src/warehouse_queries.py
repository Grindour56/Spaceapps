"""Fast FireScope summaries from DuckDB aggregates; exact AOIs filter points in DuckDB."""
from __future__ import annotations
from collections import defaultdict
from pathlib import Path
import duckdb
from analytics import aggregate_rows, _bounds


def _conditions(payload, table_alias='', date_column='date'):
    prefix=f'{table_alias}.' if table_alias else ''
    clauses=[];params=[]
    if payload.get('start'): clauses.append(f'{prefix}{date_column} >= ?');params.append(payload['start'])
    if payload.get('end'): clauses.append(f'{prefix}{date_column} <= ?');params.append(payload['end'])
    sensor=payload.get('sensor','all')
    if sensor!='all':clauses.append(f'{prefix}sensor = ?');params.append(sensor)
    satellites=payload.get('satellites')
    if satellites is not None:
        if not satellites: clauses.append('FALSE')
        else:
            clauses.append(f'{prefix}satellite IN ('+','.join('?' for _ in satellites)+')');params.extend(satellites)
    confidence=payload.get('confidence','all')
    if confidence=='high':clauses.append(f"{prefix}confidence_band='high'")
    elif confidence=='nominal':clauses.append(f"{prefix}confidence_band IN ('high','nominal')")
    elif confidence=='low':clauses.append(f"{prefix}confidence_band='low'")
    return (' WHERE '+' AND '.join(clauses) if clauses else ''),params

def _add(bucket,day,count,frp,missing,sensor,sat,daynight,confidence):
    b=bucket.setdefault(day,{'date':day,'count':0,'frp_sum_mw':0.0,'missing_frp':0,'sensors':defaultdict(int),'satellites':defaultdict(int),'daynight':defaultdict(int),'confidence':defaultdict(int)})
    b['count']+=count;b['frp_sum_mw']+=frp or 0;b['missing_frp']+=missing
    b['sensors'][sensor]+=count;b['satellites'][sat]+=count;b['daynight'][daynight]+=count;b['confidence'][confidence]+=count

def _serial(b):
    return {'date':b['date'],'count':b['count'],'frp_sum_mw':round(b['frp_sum_mw'],3),'missing_frp':b['missing_frp'],'sensors':dict(b['sensors']),'satellites':dict(b['satellites']),'daynight':dict(b['daynight'])}

def _coverage_summary_con(con, payload=None):
    payload=payload or {};clauses=[];params=[]
    if payload.get('start'): clauses.append('acquisition_date >= ?');params.append(payload['start'])
    if payload.get('end'): clauses.append('acquisition_date <= ?');params.append(payload['end'])
    where=(' WHERE '+' AND '.join(clauses)) if clauses else ''
    days=con.execute('SELECT count(DISTINCT acquisition_date) FROM observations'+where,params).fetchone()[0]
    rows=con.execute('SELECT sensor,satellite,coalesce(product_version,\'unknown\'),min(acquisition_date),max(acquisition_date),count(DISTINCT acquisition_date),count(*) FROM observations'+where+' GROUP BY 1,2,3 ORDER BY 1,2,3',params).fetchall()
    return {
        'observed_detection_dates':int(days or 0),
        'confirmed_zero_dates':0,
        'unavailable_or_unknown_dates_semantics':'Dates without records have unknown acquisition coverage, never confirmed zero fire activity.',
        'by_sensor_satellite':[{'sensor':s,'satellite':sat,'product_version':ver,'date_min':str(dmin) if dmin else None,'date_max':str(dmax) if dmax else None,'observed_dates':int(ndays),'detections':int(det)} for s,sat,ver,dmin,dmax,ndays,det in rows],
    }

def _sensor_comparison_con(con, payload=None):
    payload=payload or {};clauses=[];params=[]
    if payload.get('start'): clauses.append('date >= ?');params.append(payload['start'])
    if payload.get('end'): clauses.append('date <= ?');params.append(payload['end'])
    where=(' WHERE '+' AND '.join(clauses)) if clauses else ''
    has_table=con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='sensor_comparison'").fetchone()[0]>0
    if has_table:
        matched,m_cnt,v_cnt,m_frp,v_frp=con.execute('SELECT count(*),coalesce(sum(modis_count),0),coalesce(sum(viirs_count),0),coalesce(sum(modis_frp_sum_mw),0),coalesce(sum(viirs_frp_sum_mw),0) FROM sensor_comparison'+where,params).fetchone()
    else:
        matched=m_cnt=v_cnt=0;m_frp=v_frp=0.0
    m_cnt=int(m_cnt or 0);v_cnt=int(v_cnt or 0)
    return {
        'comparison_basis':'H3 resolution 6 + UTC acquisition date',
        'matched_cell_days':int(matched or 0),
        'modis_detections':m_cnt,
        'viirs_detections':v_cnt,
        'viirs_to_modis_count_ratio':round(v_cnt/m_cnt,4) if m_cnt else None,
        'modis_frp_sum_mw':round(float(m_frp or 0.0),3),
        'viirs_frp_sum_mw':round(float(v_frp or 0.0),3),
        'model_validated':False,
        'harmonized_estimates':None,
        'validation_note':'Descriptive co-located sensor comparison only; no cross-sensor calibration model has passed spatial/temporal holdout validation.',
    }

def get_coverage_summary(db_path, payload=None):
    con=duckdb.connect(str(Path(db_path)),read_only=True);con.execute("SET TimeZone='UTC'")
    try: return _coverage_summary_con(con,payload)
    finally: con.close()

def get_sensor_comparison(db_path, payload=None):
    con=duckdb.connect(str(Path(db_path)),read_only=True);con.execute("SET TimeZone='UTC'")
    try: return _sensor_comparison_con(con,payload)
    finally: con.close()

def _with_metadata(con,result,payload=None):
    lo,hi=con.execute('SELECT min(acquisition_date),max(acquisition_date) FROM observations').fetchone()
    result['date_min']=str(lo) if lo else None;result['date_max']=str(hi) if hi else None
    result['source_records']=con.execute('SELECT count(*) FROM observations').fetchone()[0]
    result['manifest_status']=dict(con.execute('SELECT status,count(*) FROM source_manifest GROUP BY status').fetchall())
    result['partial']=True;result['input_coverage_complete']=False
    result['run_sampled']=bool(result['manifest_status'].get('sampled',0))
    result['coverage_note']='Only detection-observed dates are represented. Sensor acquisition opportunities and confirmed zero-detection coverage are unavailable.'
    result['coverage_summary']=_coverage_summary_con(con,payload)
    result['sensor_comparison']=_sensor_comparison_con(con,payload)
    return result

def _point_rows(cursor):
    names=[x[0] for x in cursor.description]
    while rows:=cursor.fetchmany(8192):
        for values in rows:
            row=dict(zip(names,values));stamp=row.pop('acquisition_datetime_utc',None)
            row['datetime_utc']=stamp.strftime('%Y-%m-%dT%H:%M:%SZ') if stamp else None
            date=row.pop('acquisition_date',None);row['date']=str(date) if date else ''
            if 'satellite' in row: row['satellite_std']=row.pop('satellite')
            if 'confidence_raw' in row: row['confidence']=row.pop('confidence_raw')
            if 'product_version' in row: row['version']=row.pop('product_version')
            yield row

def query_warehouse(db_path,payload,map_limit=2000,grid_limit=5000):
    con=duckdb.connect(str(Path(db_path)),read_only=True);con.execute("SET TimeZone='UTC'")
    geometry=payload.get('geometry');country=payload.get('country')
    if geometry and not country:
        where,params=_conditions(payload,date_column='acquisition_date')
        clauses=[where[7:]] if where else [];values=list(params);bounds=_bounds(geometry)
        if bounds:
            minx,miny,maxx,maxy=bounds;clauses.append('latitude BETWEEN ? AND ?');values.extend([miny,maxy])
            if maxx-minx<180:clauses.append('longitude BETWEEN ? AND ?');values.extend([minx,maxx])
        sql_where=' WHERE '+' AND '.join(f'({x})' for x in clauses) if clauses else ''
        sql=('SELECT acquisition_datetime_utc,acquisition_date,latitude,longitude,sensor,satellite,instrument,confidence_raw,confidence_class,frp,brightness,bright_t31,bright_ti4,bright_ti5,daynight,detection_type,product_version,source_file FROM observations'+sql_where)
        result=aggregate_rows(_point_rows(con.execute(sql,values)),payload,map_limit=map_limit,grid_limit=grid_limit)
        result=_with_metadata(con,result,payload);con.close();return result
    aggregate_table='country_daily' if country else 'daily_activity'
    where,params=_conditions(payload)
    if country:
        where=(where+' AND country_name = ?') if where else ' WHERE country_name = ?'
        params.append(country)
    sql=(f'SELECT date,sensor,satellite,confidence_band,daynight,sum(detection_count) AS n,sum(coalesce(frp_sum_mw,0)) AS frp,sum(missing_frp) AS missing FROM {aggregate_table}'+where+' GROUP BY date,sensor,satellite,confidence_band,daynight ORDER BY date')
    rows=con.execute(sql,params).fetchall()
    by_day={};by_month={};by_year={};sensors=defaultdict(int);satellites=defaultdict(int);daynight=defaultdict(int);confidence=defaultdict(int)
    total=0;frp_total=0.0;missing_frp=0
    for day,sensor,sat,band,dn,n,frp,missing in rows:
        day=str(day);n=int(n);frp=float(frp or 0);missing=int(missing or 0);sensor=sensor or 'Unknown';sat=sat or 'Unknown';dn=dn or 'Unknown';band=band or 'unknown'
        total+=n;frp_total+=frp;missing_frp+=missing;sensors[sensor]+=n;satellites[sat]+=n;daynight[dn]+=n;confidence[band]+=n
        _add(by_day,day,n,frp,missing,sensor,sat,dn,band);_add(by_month,day[:7],n,frp,missing,sensor,sat,dn,band);_add(by_year,day[:4],n,frp,missing,sensor,sat,dn,band)
    daily=[_serial(by_day[k]) for k in sorted(by_day)];monthly=[_serial(by_month[k]) for k in sorted(by_month)];yearly=[_serial(by_year[k]) for k in sorted(by_year)]
    selected=payload.get('selected_date')
    map_where,map_params=_conditions(payload)
    map_clauses=[map_where[7:]] if map_where else []
    if country:map_clauses.append('country_name = ?');map_params.append(country)
    if selected:map_clauses.append('date = ?');map_params.append(selected)
    map_filter=' WHERE '+' AND '.join(f'({x})' for x in map_clauses) if map_clauses else ''
    map_total=con.execute('SELECT coalesce(sum(detection_count),0) FROM '+aggregate_table+map_filter,map_params).fetchone()[0]
    point_where,point_params=_conditions(payload,table_alias='o',date_column='acquisition_date');point_clauses=[point_where[7:]] if point_where else []
    if country:point_clauses.append('c.country_name = ?');point_params.append(country)
    if selected:point_clauses.append('o.acquisition_date = ?');point_params.append(selected)
    point_filter=' WHERE '+' AND '.join(f'({x})' for x in point_clauses) if point_clauses else ''
    point_from='observations o JOIN country_assignment c USING(row_fingerprint)' if country else 'observations o'
    point_sql='SELECT acquisition_datetime_utc,acquisition_date,latitude,longitude,sensor,satellite,instrument,confidence_raw,frp,brightness,bright_t31,bright_ti4,bright_ti5,daynight,detection_type,product_version,source_file FROM '+point_from+point_filter+' ORDER BY row_fingerprint LIMIT ?'
    map_points=[]
    for row in _point_rows(con.execute(point_sql,point_params+[map_limit])):
        map_points.append({k:row.get(k) for k in ('datetime_utc','date','latitude','longitude','sensor','satellite_std','instrument','confidence','frp','brightness','bright_t31','bright_ti4','bright_ti5','daynight','detection_type','version','source_file')})
    grid_table='country_grid2_daily' if country else 'grid2_daily'
    grid_where,grid_params=_conditions(payload)
    grid_clauses=[grid_where[7:]] if grid_where else []
    if country:grid_clauses.append('country_name = ?');grid_params.append(country)
    if selected:grid_clauses.append('date = ?');grid_params.append(selected)
    grid_filter=' WHERE '+' AND '.join(f'({x})' for x in grid_clauses) if grid_clauses else ''
    grid_rows=con.execute('SELECT grid_x,grid_y,sum(detection_count) AS n FROM '+grid_table+grid_filter+' GROUP BY grid_x,grid_y ORDER BY n DESC LIMIT ?',grid_params+[grid_limit]).fetchall()
    grid_total=con.execute('SELECT count(*) FROM (SELECT grid_x,grid_y FROM '+grid_table+grid_filter+' GROUP BY grid_x,grid_y)',grid_params).fetchone()[0]
    result={'total':total,'frp_sum_mw':round(frp_total,3),'missing_frp':missing_frp,'sensors':dict(sensors),'satellites':dict(satellites),'daynight':dict(daynight),'confidence_classes':dict(confidence),'daily':daily,'monthly':monthly,'yearly':yearly,'peak_day':max(daily,key=lambda x:x['count'])['date'] if daily else None,'map_points':map_points,'map_points_total':int(map_total),'map_grid':[{'latitude':int(y)*2-89,'longitude':int(x)*2-179,'count':int(n)} for x,y,n in grid_rows],'grid_cells_total':int(grid_total)}
    result=_with_metadata(con,result,payload);con.close();return result
