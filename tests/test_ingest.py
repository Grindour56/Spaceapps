import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ingest import normalize_row

class NormalizeTests(unittest.TestCase):
    def test_normalizes_modis_and_preserves_sensor_fields(self):
        row = dict(latitude='10', longitude='20', brightness='330', bright_t31='290',
                   acq_date='2024-01-02', acq_time='0030', satellite='Terra',
                   instrument='MODIS', confidence='80', frp='5.2', daynight='D', type='0')
        out = normalize_row(row, 'fire_archive_M-C61.csv')
        self.assertEqual(out['datetime_utc'], '2024-01-02T00:30:00Z')
        self.assertEqual((out['sensor'], out['satellite_std']), ('MODIS', 'Terra'))
        self.assertEqual((out['brightness'], out['bright_t31']), (330.0, 290.0))

    def test_rejects_invalid_coordinates(self):
        row = dict(latitude='91', longitude='0', acq_date='2024-01-02', acq_time='0000')
        self.assertIsNone(normalize_row(row, 'x.csv'))

    def test_normalizes_snpp_instrument_as_viirs_sensor(self):
        row = dict(latitude='1', longitude='2', acq_date='2024-01-02', acq_time='1200',
                   satellite='SNPP', instrument='SNPP')
        out = normalize_row(row, 'fire_archive_SV-C2.csv')
        self.assertEqual(out['sensor'], 'VIIRS')
        self.assertEqual(out['satellite_std'], 'Suomi-NPP')

    def test_keeps_viirs_constellation_identity(self):
        row = dict(latitude='1', longitude='2', acq_date='2024-01-02', acq_time='1200',
                   satellite='N21', instrument='VIIRS', confidence='n')
        out = normalize_row(row, 'x.csv')
        self.assertEqual((out['sensor'], out['satellite_std']), ('VIIRS', 'NOAA-21'))
        self.assertEqual(out['confidence_class'], 'nominal')

if __name__ == '__main__':
    unittest.main()
