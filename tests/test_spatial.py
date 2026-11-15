import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from spatial import point_in_geometry, prepare_geometry, prepared_contains

class PointInGeometryTests(unittest.TestCase):
    def test_prepared_geometry_matches_geojson_point_membership(self):
        geom={'type':'Polygon','coordinates':[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}
        prepared=prepare_geometry(geom)
        self.assertTrue(prepared_contains(2,2,prepared,geom))
        self.assertFalse(prepared_contains(5,2,prepared,geom))
        self.assertTrue(prepared_contains(0,2,prepared,geom))

    def test_polygon_includes_inside_excludes_outside(self):
        geom={'type':'Polygon','coordinates':[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}
        self.assertTrue(point_in_geometry(2,2,geom))
        self.assertFalse(point_in_geometry(5,2,geom))

    def test_polygon_hole_is_excluded(self):
        geom={'type':'Polygon','coordinates':[
            [[0,0],[6,0],[6,6],[0,6],[0,0]],
            [[2,2],[4,2],[4,4],[2,4],[2,2]],
        ]}
        self.assertTrue(point_in_geometry(1,1,geom))
        self.assertFalse(point_in_geometry(3,3,geom))

    def test_multipolygon_accepts_point_in_either_component(self):
        geom={'type':'MultiPolygon','coordinates':[
            [[[-5,-5],[-3,-5],[-3,-3],[-5,-3],[-5,-5]]],
            [[[3,3],[5,3],[5,5],[3,5],[3,3]]],
        ]}
        self.assertTrue(point_in_geometry(-4,-4,geom))
        self.assertTrue(point_in_geometry(4,4,geom))
        self.assertFalse(point_in_geometry(0,0,geom))

    def test_missing_geometry_means_global(self):
        self.assertTrue(point_in_geometry(12,34,None))

if __name__ == '__main__': unittest.main()
