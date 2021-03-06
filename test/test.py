from gtfs.loader import load
from gtfs.schedule import Schedule
from gtfs.types import TransitTime
from gtfs.feed import Feed
import os

import unittest

class TestDictReader(unittest.TestCase):
  def test_basic(self):
    curpath = os.path.dirname(os.path.realpath(__file__))
    feedpath = os.path.join(curpath,"data/sample-feed.zip")

    fd = Feed(feedpath)
    rd = fd.get_reader( "stops.txt" )

    self.assertEqual( rd.next(), 
      {u'stop_lat': u'37.728631', u'stop_lon': 
      u'-122.431282', u'stop_url': u'', u'stop_id': 
      u'S1', u'stop_desc': 
      u'The stop is located at the southwest corner of the intersection.', 
      u'stop_name': u'Mission St. & \u9280 Ave.', u'location_type': u''})


class TestSchedule(unittest.TestCase):
  def setUp(self):
    curpath = os.path.dirname(os.path.realpath(__file__))
    feedpath = os.path.join(curpath,"data/sample-feed.zip")

    self.schedule = load( feedpath )

  def test_routes( self ):
    self.assertEqual( self.schedule.routes[0].route_id, "A" )

  def test_service_periods( self ):
    self.assertEqual( [sp.service_id for sp in self.schedule.service_periods],
                      ["WE","WD"] )
    self.assertEqual( type( self.schedule.service_periods[0].monday ),
                      bool )

  def test_stops( self ):
    self.assertEqual( [st.stop_id for st in self.schedule.stops],
      [u'S1', u'S2', u'S3', u'S4', u'S5', u'S6', u'S7', u'S8'] )

  def test_route_trips( self ):
    self.assertEqual( [tr.trip_id for tr in self.schedule.routes[0].trips],
      [u'AWE1', u'AWD1'] )

  def test_trip_stop_times( self ):
    self.assertEqual( [(st.arrival_time,
                        st.departure_time) for st in self.schedule.routes[0].trips[0].stop_times],
                      [(TransitTime(370), TransitTime(370)), 
                      (None, None), 
                      (TransitTime(380), TransitTime(390)), 
                      (None, None), 
                      (TransitTime(405), TransitTime(405))] )

  def test_service_period_trips( self ):
    self.assertEqual( [tr.trip_id for tr in self.schedule.service_periods[0].trips],
      [u'AWE1'] )

  def test_stop_stop_times( self ):
    self.assertEqual( [(st.arrival_time.val,st.departure_time.val) for st in self.schedule.stops[0].stop_times],
      [(370, 370), (370, 370)] )

  def test_agencies( self ):
    self.assertEqual( [ag.agency_id for ag in self.schedule.agencies],
      [u'FunBus'] )

  def test_agency_routes( self ):
    self.assertEqual( [rt.route_id for rt in self.schedule.agencies[0].routes],
      [] )

if __name__=='__main__':
  unittest.main()
