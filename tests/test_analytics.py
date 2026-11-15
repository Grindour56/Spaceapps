import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from analytics import aggregate_rows

ROWS=[
 {'date':'2024-06-01','datetime_utc':'2024-06-01T10:00:00Z','latitude':1.0,'longitude':1.0,'sensor':'MODIS','satellite_std':'Terra','confidence':'90','confidence_class':'numeric','frp':5.0,'daynight':'D','brightness':330.0,'bright_t31':290.0},
 {'date':'2024-06-01','datetime_utc':'2024-06-01T11:00:00Z','latitude':2.0,'longitude':2.0,'sensor':'VIIRS','satellite_std':'NOAA-20','confidence':'h','confidence_class':'high','frp':3.0,'daynight':'N','brightness':310.0,'bright_t31':280.0},
 {'date':'2024-06-02','datetime_utc':'2024-06-02T11:00:00Z','latitude':8.0,'longitude':8.0,'sensor':'VIIRS','satellite_std':'NOAA-21','confidence':'n','confidence_class':'nominal','frp':2.0,'daynight':'D','brightness':300.0,'bright_t31':270.0},
]
class AggregateTests(unittest.TestCase):
    def test_aggregates_period_and_day_by_sensor(self):
        got=aggregate_rows(ROWS,{},map_limit=10)
        self.assertEqual(got['total'],3)
        self.assertEqual(got['frp_sum_mw'],10.0)
        self.assertEqual(got['sensors'],{'MODIS':1,'VIIRS':2})
        self.assertEqual(got['daily'][0]['date'],'2024-06-01')
        self.assertEqual(got['daily'][0]['sensors'],{'MODIS':1,'VIIRS':1})

    def test_aoi_and_date_filters_are_exact_and_stats_not_sampled(self):
        geom={'type':'Polygon','coordinates':[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}
        got=aggregate_rows(ROWS,{'geometry':geom,'start':'2024-06-01','end':'2024-06-01'},map_limit=1)
        self.assertEqual(got['total'],2)
        self.assertEqual(len(got['map_points']),1)
        self.assertEqual(got['daily'][0]['date'],'2024-06-01')

    def test_empty_satellite_selection_returns_no_records(self):
        got=aggregate_rows(ROWS,{'satellites':[]},map_limit=10)
        self.assertEqual(got['total'],0)

    def test_selected_day_filters_map_not_period_totals(self):
        got=aggregate_rows(ROWS,{'selected_date':'2024-06-02'},map_limit=10)
        self.assertEqual(got['total'],3)
        self.assertEqual(got['map_points_total'],1)
        self.assertEqual(got['map_points'][0]['date'],'2024-06-02')

    def test_missing_frp_count_is_available_per_day(self):
        row=dict(ROWS[0]);row['date']='2024-06-03';row['frp']=None
        got=aggregate_rows(ROWS+[row],{},map_limit=10)
        self.assertEqual(got['missing_frp'],1)
        self.assertEqual(next(d for d in got['daily'] if d['date']=='2024-06-03')['missing_frp'],1)

    def test_confidence_filter_respects_native_numeric_and_classes(self):
        got=aggregate_rows(ROWS,{'confidence':'high'},map_limit=10)
        self.assertEqual(got['total'],2)
        got=aggregate_rows(ROWS,{'confidence':'nominal'},map_limit=10)
        self.assertEqual(got['total'],3)

if __name__ == '__main__': unittest.main()
