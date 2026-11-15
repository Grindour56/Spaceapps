import sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from data_pipeline import connect_database,normalize_observation,merge_records,finalize_warehouse
from warehouse_queries import query_warehouse,get_coverage_summary,get_sensor_comparison

BASE=dict(latitude='10',longitude='20',brightness='330',bright_t31='290',acq_date='2024-01-02',acq_time='0030',satellite='Terra',instrument='MODIS',confidence='80',frp='5.2',daynight='D',type='0',version='6.1',scan='1.0',track='1.2')

class WarehouseQueryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.db=self.root/'f.duckdb'
        con=connect_database(self.db)
        records=[]
        for raw,path in [(BASE,'modis.csv'),(dict(BASE,instrument='VIIRS',satellite='SNPP',confidence='nominal',version='2.0'), 'viirs.csv')]:
            row,errors=normalize_observation(raw,path);self.assertEqual(errors,[]);records.append((path,row))
        for path,row in records:merge_records(con,path,'fp-'+path,[row])
        con.close();finalize_warehouse(self.db,self.root,export=False)
    def tearDown(self):self.tmp.cleanup()
    def test_aggregate_totals_and_visual_sample_are_separate(self):
        result=query_warehouse(self.db,{'sensor':'all','satellites':['Terra','Suomi-NPP'],'confidence':'all'},map_limit=1)
        self.assertEqual(result['total'],2);self.assertEqual(len(result['map_points']),1)
        self.assertEqual(result['map_points_total'],2);self.assertEqual(result['sensors'],{'MODIS':1,'VIIRS':1})
        self.assertEqual(result['daily'][0]['count'],2);self.assertEqual(len(result['map_grid']),1)
        focused=query_warehouse(self.db,{'sensor':'all','satellites':['Terra','Suomi-NPP'],'confidence':'all','selected_date':'2024-01-02'},map_limit=1)
        self.assertEqual(focused['map_points_total'],2);self.assertEqual(focused['map_grid'][0]['count'],2)
    def test_sensor_confidence_and_date_filters(self):
        result=query_warehouse(self.db,{'sensor':'VIIRS','satellites':['Suomi-NPP'],'confidence':'nominal','start':'2024-01-02','end':'2024-01-02'})
        self.assertEqual(result['total'],1);self.assertEqual(result['sensors'],{'VIIRS':1})
    def test_country_assignment_uses_local_polygon_boundaries(self):
        import json
        from data_pipeline import assign_countries
        boundary=self.root/'countries.geojson'
        boundary.write_text(json.dumps({'type':'FeatureCollection','features':[{'type':'Feature','id':'001','properties':{'name':'Testland'},'geometry':{'type':'Polygon','coordinates':[[[19,9],[21,9],[21,11],[19,11],[19,9]]]}}]}))
        con=connect_database(self.db)
        status=assign_countries(con,boundary)
        self.assertEqual(status['status'],'ok')
        self.assertEqual(con.execute("select count(*) from country_assignment where country_name='Testland'").fetchone()[0],2)
        con.close()

    def test_country_selection_uses_precomputed_country_tables(self):
        import json
        boundary=self.root/'countries.geojson'
        boundary.write_text(json.dumps({'type':'FeatureCollection','features':[{'type':'Feature','id':'001','properties':{'name':'Testland'},'geometry':{'type':'Polygon','coordinates':[[[19,9],[21,9],[21,11],[19,11],[19,9]]]}}]}))
        finalize_warehouse(self.db,self.root,boundary_path=boundary,export=False)
        geom={'type':'Polygon','coordinates':[[[19,9],[21,9],[21,11],[19,11],[19,9]]]}
        result=query_warehouse(self.db,{'country':'Testland','geometry':geom,'sensor':'all','satellites':['Terra','Suomi-NPP'],'confidence':'all'},map_limit=1)
        self.assertEqual(result['total'],2);self.assertEqual(result['map_points_total'],2);self.assertEqual(len(result['map_points']),1)
        self.assertEqual(result['map_grid'][0]['count'],2)

    def test_aoi_query_is_exact_and_uses_point_rows(self):
        geom={'type':'Polygon','coordinates':[[[19,9],[21,9],[21,11],[19,11],[19,9]]]}
        result=query_warehouse(self.db,{'geometry':geom,'sensor':'all','satellites':['Terra','Suomi-NPP'],'confidence':'all'})
        self.assertEqual(result['total'],2)
        outside={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}
        self.assertEqual(query_warehouse(self.db,{'geometry':outside})['total'],0)

    def test_coverage_and_sensor_comparison_summaries_are_exposed_honestly(self):
        result=query_warehouse(self.db,{'sensor':'all','satellites':['Terra','Suomi-NPP'],'confidence':'all'})
        self.assertIn('coverage_summary',result)
        self.assertEqual(result['coverage_summary']['observed_detection_dates'],1)
        self.assertEqual(result['coverage_summary']['confirmed_zero_dates'],0)
        self.assertIn('sensor_comparison',result)
        self.assertEqual(result['sensor_comparison']['matched_cell_days'],1)
        self.assertEqual(result['sensor_comparison']['modis_detections'],1)
        self.assertEqual(result['sensor_comparison']['viirs_detections'],1)
        self.assertFalse(result['sensor_comparison']['model_validated'])
        self.assertIsNone(result['sensor_comparison']['harmonized_estimates'])
        cov=get_coverage_summary(self.db)
        comp=get_sensor_comparison(self.db)
        self.assertEqual(cov['observed_detection_dates'],1)
        self.assertEqual(comp['matched_cell_days'],1)

    def test_parquet_exports_match_duckdb_tables_and_reruns_are_idempotent(self):
        import duckdb
        first=finalize_warehouse(self.db,self.root,export=True)
        second=finalize_warehouse(self.db,self.root,export=True)
        self.assertEqual(first['observations'],2)
        self.assertEqual(second['observations'],2)
        parquet_obs=self.root/'data'/'processed'/'parquet'/'year=2024'/'observations.parquet'
        self.assertTrue(parquet_obs.is_file())
        with duckdb.connect() as con:
            n_parquet=con.execute(f"SELECT count(*) FROM read_parquet('{parquet_obs.as_posix()}')").fetchone()[0]
            self.assertEqual(n_parquet,2)
            for table in ('daily_activity','monthly_activity','annual_activity','h3_daily','coverage_report','sensor_comparison'):
                p=self.root/'data'/'processed'/'aggregates'/f'{table}.parquet'
                self.assertTrue(p.is_file(),table)

    def test_vercel_wsgi_and_handler_entrypoints_serve_routes_and_block_secrets(self):
        import io, json, tomllib
        from http.server import BaseHTTPRequestHandler
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
        import server as srv
        self.assertTrue(hasattr(srv,'app'))
        self.assertTrue(callable(srv.app))
        self.assertTrue(hasattr(srv,'handler'))
        self.assertTrue(issubclass(srv.handler,BaseHTTPRequestHandler))
        pyproject=Path(__file__).resolve().parents[1]/'pyproject.toml'
        self.assertTrue(pyproject.is_file())
        cfg=tomllib.loads(pyproject.read_text())
        self.assertEqual(cfg.get('tool',{}).get('vercel',{}).get('entrypoint'),'app/server.py')

        def call_wsgi(method,path,body=b''):
            status_box=[];headers_box=[]
            def start_response(status,headers,exc_info=None):
                status_box.append(status);headers_box.extend(headers)
            env={'REQUEST_METHOD':method,'PATH_INFO':path,'CONTENT_LENGTH':str(len(body)),'wsgi.input':io.BytesIO(body)}
            chunks=b''.join(srv.app(env,start_response))
            return status_box[0],dict(headers_box),chunks

        st,_,body=call_wsgi('GET','/api/health')
        self.assertTrue(st.startswith('200'))
        self.assertTrue(json.loads(body)['ok'])
        st,_,body=call_wsgi('POST','/api/analytics',json.dumps({'sensor':'MODIS'}).encode())
        self.assertTrue(st.startswith('200'))
        self.assertIn('total',json.loads(body))
        st,_,_=call_wsgi('GET','/map_key')
        self.assertTrue(st.startswith('404'))

if __name__=='__main__':unittest.main()
