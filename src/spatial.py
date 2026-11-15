"""Small GeoJSON point-in-polygon helpers; Shapely accelerates repeated queries."""
try:
    import shapely
    from shapely.geometry import shape as _shape
    _HAS_SHAPELY=True
except ImportError:
    shapely=None; _shape=None; _HAS_SHAPELY=False


def _on_segment(x, y, a, b, eps=1e-10):
    ax, ay = a[:2]; bx, by = b[:2]
    cross = (x-ax)*(by-ay) - (y-ay)*(bx-ax)
    if abs(cross) > eps: return False
    return min(ax,bx)-eps <= x <= max(ax,bx)+eps and min(ay,by)-eps <= y <= max(ay,by)+eps

def _ring_contains(x, y, ring):
    if not ring or len(ring)<3: return False
    inside=False
    prev=ring[-1]
    # Shift longitudes across the antimeridian to the query point's neighborhood.
    px=x
    py=y
    for cur in ring:
        ax,ay=prev[:2]; bx,by=cur[:2]
        while ax-px>180: ax-=360
        while ax-px < -180: ax+=360
        while bx-px>180: bx-=360
        while bx-px < -180: bx+=360
        if _on_segment(px,py,(ax,ay),(bx,by)): return True
        if ((ay>py)!=(by>py)) and px < (bx-ax)*(py-ay)/(by-ay)+ax:
            inside=not inside
        prev=cur
    return inside

def _polygon_contains(x,y,rings):
    if not rings or not _ring_contains(x,y,rings[0]): return False
    return not any(_ring_contains(x,y,hole) for hole in rings[1:])

def point_in_geometry(longitude, latitude, geometry):
    """Return True for a point in GeoJSON Polygon/MultiPolygon; boundary counts inside."""
    if geometry is None: return True
    if not isinstance(geometry,dict): return False
    kind=geometry.get('type'); coords=geometry.get('coordinates') or []
    if kind=='Polygon': return _polygon_contains(longitude,latitude,coords)
    if kind=='MultiPolygon': return any(_polygon_contains(longitude,latitude,p) for p in coords)
    if kind=='Feature': return point_in_geometry(longitude,latitude,geometry.get('geometry'))
    return False

def prepare_geometry(geometry):
    """Prepare a validated GeoJSON geometry once for repeated point membership tests."""
    if geometry is None: return None
    if _HAS_SHAPELY:
        geom=_shape(geometry)
        shapely.prepare(geom)
        return geom
    return geometry

def prepared_contains(longitude,latitude,prepared,original=None):
    """Boundary-inclusive exact membership, accelerated by Shapely when installed."""
    if prepared is None: return True
    if _HAS_SHAPELY and not isinstance(prepared,dict):
        return bool(shapely.intersects_xy(prepared,longitude,latitude))
    return point_in_geometry(longitude,latitude,original if original is not None else prepared)
