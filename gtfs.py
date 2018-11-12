from zipfile import ZipFile
import pandas as pd
from shapely.geometry import LineString, Point
from shapely.ops import transform
import numpy as np

def parse_stoptime_str(ststr):
    if ststr.strip()=="":
        return None
        
    comps = ststr.split(":")
    return int(comps[0])*3600 + int(comps[1])*60 + int(comps[2])

class StopTimeLinRefer:
    def __init__( self, gtfs, p ):
        """takes gtfs feed, projection"""
        
        self.proj_shapes = {shape_id:transform(p,shape) for shape_id, shape in gtfs.shapes.items()}
        
        df = gtfs.trips[["trip_id","shape_id"]].set_index("trip_id")
        self.trip_shapes = df["shape_id"].to_dict()
        
        stop_lons, stop_lats = gtfs.stops[["stop_lon","stop_lat"]].values.T
        stop_xs, stop_ys = p( stop_lons, stop_lats )
        coords = [Point(x) for x in np.stack( [stop_xs, stop_ys] ).T]
        
        self.stop_coords = dict( zip( gtfs.stops.stop_id.values, coords ) )
        
        self.shape_stop_cache = {}
    
    def stop_dist( self, trip_id, stop_id ):
        shape_id = self.trip_shapes[ trip_id ]
        
        if (shape_id,stop_id) in self.shape_stop_cache:
            return self.shape_stop_cache[ (shape_id,stop_id) ]
        
        shape = self.proj_shapes[ shape_id ]
        
        coord = self.stop_coords[ stop_id ]
        
        ret = shape.project( coord )
        self.shape_stop_cache[ (shape_id, stop_id) ] = ret
        return ret

class GTFS:
    def __init__(self, fn):
        self.zf = ZipFile( fn )
        
        self.linrefed = False
        
    def get_shape_points(self):
        fp = self.zf.open("shapes.txt")
        return pd.read_csv(fp, dtype={"shape_id":str})
    
    def get_shapes(self):
        df = self.get_shape_points()
        
        shapes = {}
        
        groups = self.shape_points.sort_values("shape_pt_sequence").groupby("shape_id")
        for shape_id, shape_pts in groups:
            shape = LineString( shape_pts[ ["shape_pt_lon", "shape_pt_lat"] ].values )

            shapes[shape_id] = shape

        return shapes
    
    @property
    def shapes(self):
        if not hasattr(self, "_shapes"):
            self._shapes = self.get_shapes()
        return self._shapes
    
    @property
    def shape_points(self):
        if not hasattr(self, "_shape_points"):
            self._shape_points = self.get_shape_points()
        return self._shape_points
    
    def get_trips(self):
        fp = self.zf.open("trips.txt")
        return pd.read_csv(fp, dtype={"trip_id":str, "shape_id":str, "route_id":str})
    
    def get_trip( self, trip_id ):
        return self.trips[ self.trips.trip_id==trip_id ].iloc[0].to_dict()
    
    def get_trip_stoptimes( self, trip_id ):
        if not hasattr(self, "_tripid_stoptimes" ):
            self._tripid_stoptimes = self.stoptimes.set_index("trip_id")
        
        return self._tripid_stoptimes.loc[ trip_id ]
    
    def get_stoptimes(self):
        fp = self.zf.open("stop_times.txt")
        
        df = pd.read_csv(fp, dtype={"trip_id":str, "stop_id":str})
        df["arrival_time"] = df["arrival_time"].map(parse_stoptime_str)
        df["departure_time"] = df["departure_time"].map(parse_stoptime_str)
        
        return df
    
    def get_stops(self):
        fp = self.zf.open("stops.txt")
        return pd.read_csv(fp, dtype={"stop_id":str})
    
    def get_routes(self):
        fp = self.zf.open("routes.txt")
        return pd.read_csv(fp, dtype={"route_id":str})
    
    def get_service_ids(self):
        fp = self.zf.open("calendar.txt")
        return pd.read_csv(fp)
    
    @property
    def trips(self):
        if not hasattr(self, "_trips"):
            self._trips = self.get_trips()
        return self._trips
    
    @property
    def stoptimes(self):
        if not hasattr(self, "_stoptimes"):
            self._stoptimes = self.get_stoptimes()
        return self._stoptimes
    
    @property
    def stops(self):
        if not hasattr(self, "_stops"):
            self._stops = self.get_stops()
        return self._stops
    
    @property
    def routes(self):
        if not hasattr(self, "_routes"):
            self._routes = self.get_routes()
        return self._routes
    
    def set_shape_dist_traveled( self, p ):
        try:
            delattr(self, "_tripid_stoptimes")
        except AttributeError:
            pass
        
        stlr = StopTimeLinRefer(self, p)
        
        st_dists = self.stoptimes.apply( lambda x: stlr.stop_dist(x.trip_id, x.stop_id), axis=1 )
        
        self.stoptimes["shape_dist_traveled"] = st_dists
        self.linrefed = True
    
