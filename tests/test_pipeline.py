import sys, tempfile, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from data_pipeline import normalize_observation, exact_fingerprint, overlap_signature, connect_database, merge_records, process_csv, discover_inputs

BASE=dict(latitude='10',longitude='20',brightness='330',bright_t31='290',acq_date='2024-01-02',acq_time='0030',satellite='Terra',instrument='MODIS',confidence='80',frp='5.2',daynight='D',type='0',version='6.1',scan='1.0',track='1.2')

class PipelineTests(unittest.TestCase):
    def test_normalization_preserves_native_and_original_fields(self):
        out,errors=normalize_observation(BASE,'fire_archive_M-C61.csv')
        self.assertEqual(errors,[])
        self.assertEqual(out['acquisition_datetime_utc'],'2024-01-02T00:30:00Z')
        self.assertEqual(out['acquisition_time_original'],'0030')
        self.assertEqual(out['sensor'],'MODIS')
        self.assertEqual(out['satellite'],'Terra')
        self.assertEqual(out['brightness'],330.0)
        self.assertEqual(out['bright_t31'],290.0)
        self.assertIsNone(out['bright_ti4'])
        self.assertIn('bright_t31',out['raw_record_json'])

    def test_exact_duplicate_hash_ignores_file_path_but_nearby_is_distinct(self):
        a,_=normalize_observation(BASE,'one.csv')
        b,_=normalize_observation(BASE,'two.csv')
        nearby=dict(BASE,latitude='10.0001')
        c,_=normalize_observation(nearby,'two.csv')
        self.assertEqual(exact_fingerprint(a),exact_fingerprint(b))
        self.assertNotEqual(exact_fingerprint(a),exact_fingerprint(c))
        self.assertEqual(overlap_signature(a),overlap_signature(b))

    def test_incremental_merge_deduplicates_and_reruns_are_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn=connect_database(Path(tmp)/'fires.duckdb')
            row,_=normalize_observation(BASE,'a.csv')
            first=merge_records(conn,'a.csv','fp-a',[row])
            row_same,_=normalize_observation(BASE,'b.csv')
            row_near,_=normalize_observation(dict(BASE,latitude='10.0001'),'b.csv')
            second=merge_records(conn,'b.csv','fp-b',[row_same,row_near])
            self.assertEqual(first['retained_records'],1)
            self.assertEqual(second['exact_duplicates'],1)
            self.assertEqual(conn.execute('select count(*) from observations').fetchone()[0],2)
            repeated=merge_records(conn,'b.csv','fp-b',[row_same,row_near])
            self.assertTrue(repeated['skipped'])
            self.assertEqual(conn.execute('select count(*) from observations').fetchone()[0],2)
            conn.close()

    def test_sampled_manifest_upgrades_to_full_processing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'fire_archive_M-C61.csv'
            fields=list(BASE);rows=[BASE,dict(BASE,latitude='11')]
            with source.open('w',newline='') as f:
                import csv
                writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
            db=root/'warehouse.duckdb'
            sampled=process_csv(source,db,root=root,limit_rows=1)
            self.assertEqual(sampled['status'],'sampled')
            full=process_csv(source,db,root=root)
            self.assertEqual(full['status'],'complete')
            con=connect_database(db)
            self.assertEqual(con.execute('select count(*) from observations').fetchone()[0],2)
            self.assertEqual(con.execute("select status from source_manifest where source_path='fire_archive_M-C61.csv'").fetchone()[0],'complete')
            con.close()

    def test_zip_member_ingestion_does_not_extract_and_records_provenance(self):
        import csv,io,zipfile
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);archive=root/'manual.zip';buffer=io.StringIO(newline='')
            writer=csv.DictWriter(buffer,fieldnames=list(BASE));writer.writeheader();writer.writerow(BASE)
            with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as zf:zf.writestr('fire_archive_M-C61_member.csv',buffer.getvalue())
            db=root/'warehouse.duckdb'
            from data_pipeline import process_zip_member
            result=process_zip_member(archive,'fire_archive_M-C61_member.csv',db,root=root)
            self.assertEqual(result['status'],'complete')
            con=connect_database(db)
            self.assertEqual(con.execute('select count(*) from observations').fetchone()[0],1)
            self.assertEqual(con.execute('select source_path from observations').fetchone()[0],'manual.zip!fire_archive_M-C61_member.csv')
            con.close()

    def test_invalid_coordinates_are_recorded_as_validation_errors(self):
        row=dict(BASE,latitude='91')
        out,errors=normalize_observation(row,'bad.csv')
        self.assertIsNone(out)
        self.assertIn('latitude_out_of_range',errors)

    def test_exact_duplicate_hash_ignores_source_product_label(self):
        archive,_=normalize_observation(BASE,'fire_archive_M-C61.csv')
        country,_=normalize_observation(BASE,'modis/2020/modis_2020_example.csv')
        self.assertNotEqual(archive['source_product'],country['source_product'])
        self.assertEqual(exact_fingerprint(archive),exact_fingerprint(country))

    def test_reprocessing_source_updates_canonical_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            con=connect_database(Path(tmp)/'x.duckdb')
            a,_=normalize_observation(BASE,'fire_archive_M-C61.csv')
            b,_=normalize_observation(BASE,'modis/2020/modis_2020_example.csv')
            merge_records(con,a['source_path'],'fp-a',[a]);merge_records(con,b['source_path'],'fp-b',[b])
            refreshed=merge_records(con,a['source_path'],'fp-a',[a],force=True)
            self.assertEqual(refreshed['exact_duplicates'],1)
            self.assertEqual(con.execute('select source_path from observations').fetchone()[0],b['source_path'])
            self.assertEqual(con.execute('select count(*) from observation_sources').fetchone()[0],2)
            con.close()

    def test_csv_ingestion_records_invalid_rows_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            source=root/'fire_archive_M-C61_test.csv'
            fields=list(BASE)
            rows=[BASE,BASE,dict(BASE,latitude='91')]
            with source.open('w',newline='') as f:
                import csv
                writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
            db=root/'warehouse.duckdb'
            first=process_csv(source,db,root=root)
            self.assertEqual(first['input_records'],3)
            self.assertEqual(first['invalid_records'],1)
            self.assertEqual(first['exact_duplicates'],1)
            self.assertEqual(first['new_canonical_records'],1)
            second=process_csv(source,db,root=root)
            self.assertTrue(second['skipped'])
            con=connect_database(db)
            self.assertEqual(con.execute('select count(*) from observations').fetchone()[0],1)
            self.assertEqual(con.execute('select count(*) from invalid_observations').fetchone()[0],1)
            con.close()

    def test_discover_inputs_excludes_reports_and_non_raw_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            raw_csv=root/'fire_archive_M-C61.csv'
            raw_csv.write_text('latitude,longitude\n10,20\n')
            for report in ('DATA_INVENTORY.csv','COVERAGE_REPORT.csv','PROCESSING_MANIFEST.csv','DUPLICATE_RECORDS.csv','HARMONIZATION_COMPARISON.csv'):
                (root/report).write_text('a,b\n1,2\n')
            for sub in ('outputs','warehouse','data/processed','app','tests','docs','src'):
                d=root/sub;d.mkdir(parents=True,exist_ok=True)
                (d/'ignored.csv').write_text('latitude,longitude\n10,20\n')
            csvs,zips=discover_inputs(root)
            self.assertEqual(csvs,[raw_csv.resolve()])
            self.assertEqual(zips,[])

    def test_manifest_timestamps_queryable_without_pytz_and_fast_batch_merge(self):
        import time
        with tempfile.TemporaryDirectory() as tmp:
            con=connect_database(Path(tmp)/'fast.duckdb')
            rows=[normalize_observation(dict(BASE,latitude=str(10+i*0.001)),'batch.csv')[0] for i in range(1500)]
            t0=time.monotonic()
            res=merge_records(con,'batch.csv','fp-batch',rows,invalid_records=[{'row_number':1501,'raw_record_json':'{}','validation_errors':'["lat"]'}])
            elapsed=time.monotonic()-t0
            self.assertEqual(res['retained_records'],1500)
            self.assertLess(elapsed,1.5)
            manifest_rows=con.execute('SELECT started_at, completed_at FROM source_manifest').fetchall()
            self.assertEqual(len(manifest_rows),1)
            self.assertIsNotNone(manifest_rows[0][0])
            invalid_rows=con.execute('SELECT recorded_at FROM invalid_observations').fetchall()
            self.assertEqual(len(invalid_rows),1)
            con.close()

    def test_audit_reports_and_inventory_distinguish_point_and_summary_schemas(self):
        import csv
        from audit_data import write_reports
        from data_pipeline import finalize_warehouse
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            viirs_csv=root/'fire_archive_J1V-C2_test.csv'
            with viirs_csv.open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(BASE));w.writeheader()
                w.writerow(dict(BASE,instrument='VIIRS',satellite='N20',confidence='n',version='2'))
            summary_csv=root/'country_yearly_summary.csv'
            summary_csv.write_text('country,year,total_fires\nTestland,2024,100\n')
            db=root/'warehouse'/'firescope.duckdb'
            process_csv(viirs_csv,db,root=root)
            finalize_warehouse(db,root,export=True)
            out=write_reports(root,db)
            with (root/'DATA_INVENTORY.csv').open() as inv_f:
                inv_rows=list(csv.DictReader(inv_f))
            by_path={r['path']:r for r in inv_rows}
            self.assertEqual(by_path['country_yearly_summary.csv']['observation_type'],'summary_or_unknown_schema')
            self.assertEqual(by_path['fire_archive_J1V-C2_test.csv']['observation_type'],'point_observations')
            self.assertIn('VIIRS',by_path['fire_archive_J1V-C2_test.csv']['notes'])
            self.assertNotIn('MODIS-native',by_path['fire_archive_J1V-C2_test.csv']['notes'])
            for name in ('DATA_INVENTORY.csv','DATA_AUDIT.md','PROCESSING_STATUS.md','DUPLICATE_REPORT.md','HARMONIZATION_VALIDATION.md','DATA_QUALITY_REPORT.json','COVERAGE_REPORT.csv','PROCESSING_MANIFEST.csv'):
                self.assertTrue((root/name).is_file(),name)

if __name__=='__main__': unittest.main()
