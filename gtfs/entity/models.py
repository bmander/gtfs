from datetime import date

import sqlalchemy
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Integer, Float, Date, Boolean

from gtfs.types import TransitTime


def make_boolean(value):
    if value == '':
        return None
    elif value not in ['0', '1']:
        raise ValueError
    else:
        return bool(int(value))


def make_date(value):
    return date(int(value[0:4]), int(value[4:6]),
                int(value[6:8]))


def make_time(value):
    return TransitTime(value)


class TransitTimeType(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.types.Integer

    def process_bind_param(self, value, dialect):
        return value.val if value else None

    def process_result_value(self, value, dialect):
        return TransitTime(value)  # if value else None


Base = declarative_base()


class Entity():
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if v == '' or v is None:
                v = None
            elif hasattr(self, 'inbound_conversions') and \
                     k in self.inbound_conversions:
                v = self.inbound_conversions[k](v)
            setattr(self, k, v)


class ShapePoint(Entity, Base):
    __tablename__ = "shapes"

    inbound_conversions = {'shape_pt_lat': float,
                           'shape_pt_lon': float}

    id = Column(Integer, primary_key=True)
    shape_id = Column(String, nullable=False)
    shape_pt_lat = Column(Float, nullable=False)
    shape_pt_lon = Column(Float, nullable=False)
    shape_pt_sequence = Column(Integer, nullable=False)
    shape_dist_traveled = Column(String)

    def __repr__(self):
        return "<ShapePoint #%s (%s, %s)>" % (self.shape_pt_sequence,
                                              self.shape_pt_lat,
                                              self.shape_pt_lon)


class Agency(Entity, Base):
    __tablename__ = "agency"

    agency_id = Column(String, primary_key=True)
    agency_name = Column(String, nullable=False)
    agency_url = Column(String, nullable=False)
    agency_timezone = Column(String, nullable=False)
    agency_lang = Column(String(2))
    agency_phone = Column(String)

    def __repr__(self):
        return "<Agency %s>" % self.agency_id

    def __init__(self, **kwargs):
        Entity.__init__(self, **kwargs)

        if not hasattr(self, "agency_id") or self.agency_id is None:
            self.agency_id = "__DEFAULT__"


class ServicePeriod(Entity, Base):
    __tablename__ = "calendar"

    service_id = Column(String, primary_key=True, nullable=False)
    monday = Column(Boolean, nullable=False)
    tuesday = Column(Boolean, nullable=False)
    wednesday = Column(Boolean, nullable=False)
    thursday = Column(Boolean, nullable=False)
    friday = Column(Boolean, nullable=False)
    saturday = Column(Boolean, nullable=False)
    sunday = Column(Boolean, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    inbound_conversions = {'monday': make_boolean,
                           'tuesday': make_boolean,
                           'wednesday': make_boolean,
                           'thursday': make_boolean,
                           'friday': make_boolean,
                           'saturday': make_boolean,
                           'sunday': make_boolean,
                           'start_date': make_date,
                           'end_date': make_date}

    def __repr__(self):
        return "<ServicePeriod %s %s%s%s%s%s%s%s>" % (self.service_id,
                                                      self.monday,
                                                      self.tuesday,
                                                      self.wednesday,
                                                      self.thursday,
                                                      self.friday,
                                                      self.saturday,
                                                      self.sunday)

    def active_on_dow(self, weekday):
        days = [self.monday, self.tuesday, self.wednesday,
                        self.thursday, self.friday, self.saturday,
                        self.sunday]
        return days[weekday]

    def active_on_date(self, service_date):
        exception_add = ServiceException.query.with_parent(
            self).filter_by(date=service_date,
                            exception_type='1').count() > 0

        within_period = (self.start_date <= service_date and \
                         service_date <= self.end_date)
        active_on_day = self.active_on_dow(service_date.weekday())
        exception_remove = ServiceException.query.with_parent(
            self).filter_by(date=service_date,
                            exception_type='2').count() > 0

        if exception_add:
            return True
        elif within_period and active_on_day and not exception_remove:
            return True
        else:
            return False


class ServiceException(Entity, Base):
    __tablename__ = "calendar_dates"

    service_id = Column(String, ForeignKey("calendar.service_id"),
                        primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)
    exception_type = Column(Integer, nullable=False)

    service_period = relationship(ServicePeriod, backref="exceptions")

    inbound_conversions = {'date': make_date,
                           'exception_type': int}

    def __repr__(self):
        return "<ServiceException %s %s>" % (self.date, self.exception_type)


class Route(Entity, Base):
    __tablename__ = "routes"

    route_id = Column(String, primary_key=True, nullable=False)
    agency_id = Column(String, ForeignKey("agency.agency_id"), index=True)
    route_short_name = Column(String)
    route_long_name = Column(String)
    route_desc = Column(String)
    route_type = Column(Integer, nullable=False)
    route_url = Column(String)
    route_color = Column(String(6))
    route_text_color = Column(String(6))

    agency = relationship("Agency", backref="routes")

    inbound_conversions = {'route_type': int}

    def __repr__(self):
        return "<Route %s>" % self.route_id

    def __init__(self, **kwargs):
        Entity.__init__(self, **kwargs)

        if not hasattr(self, "agency_id") or self.agency_id is None:
            self.agency_id = "__DEFAULT__"


class Stop(Entity, Base):
    __tablename__ = "stops"

    stop_id = Column(String, primary_key=True, nullable=False)
    stop_code = Column(String)
    stop_name = Column(String, nullable=False)
    stop_desc = Column(String)
    stop_lat = Column(Float, nullable=False)
    stop_lon = Column(Float, nullable=False)
    zone_id = Column(String)
    stop_url = Column(String)
    location_type = Column(Integer)
    parent_station = Column(String, ForeignKey("stops.stop_id"), index=True)

    parent = relationship("Stop", backref="child_stations",
                          remote_side=[stop_id])

    inbound_conversions = {'stop_lat': float,
                           'stop_lon': float,
                           'location_type': int}

    def __repr__(self):
        return "<Stop %s>" % self.stop_id


class Trip(Entity, Base):
    __tablename__ = "trips"

    route_id = Column(String, ForeignKey("routes.route_id"),
                      index=True, nullable=False)
    service_id = Column(String, ForeignKey("calendar.service_id"),
                        index=True, nullable=False)
    trip_id = Column(String, primary_key=True, nullable=False)
    trip_headsign = Column(String)
    trip_short_name = Column(String)
    direction_id = Column(Integer)
    block_id = Column(String)
    shape_id = Column(String)

    route = relationship("Route", backref="trips")
    service_period = relationship("ServicePeriod", backref="trips")
    stop_times = relationship("StopTime", order_by="StopTime.stop_sequence")

    inbound_conversions = {'direction_id': int}

    def __repr__(self):
        return "<Trip %s>" % self.trip_id


class StopTime(Entity, Base):
    __tablename__ = "stop_times"

    id = Column(Integer, primary_key=True)
    trip_id = Column(String, ForeignKey("trips.trip_id"),
                     index=True, nullable=False)
    arrival_time = Column(TransitTimeType, nullable=False)
    departure_time = Column(TransitTimeType, nullable=False)
    stop_id = Column(String, ForeignKey("stops.stop_id"),
                     index=True, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    shape_dist_traveled = Column(String)

    trip = relationship(Trip)
    stop = relationship(Stop, backref="stop_times")

    inbound_conversions = {'arrival_time': make_time,
                           'departure_time': make_time,
                           'stop_sequence': int,
                           'pickup_type': int,
                           'drop_off_type': int}

    def __repr__(self):
        return "<StopTime %s %s>" % (self.trip_id, self.departure_time)


class Fare(Entity, Base):
    __tablename__ = "fare_attributes"

    fare_id = Column(String, primary_key=True, nullable=False)
    price = Column(String, nullable=False)
    currency_type = Column(String(3), nullable=False)
    payment_method = Column(Integer, nullable=False)
    transfers = Column(Integer, nullable=False)
    transfer_duration = Column(Integer)

    inbound_conversions = {'payment_method': int,
                           'transfers': int,
                           'transfer_duration': int}

    def __repr__(self):
        return "<Fare %s %s>" % (self.price, self.currency_type)


class FareRule(Entity, Base):
    __tablename__ = "fare_rules"

    id = Column(Integer, primary_key=True)
    fare_id = Column(String, ForeignKey("fare_attributes.fare_id"),
                     index=True, nullable=False)
    route_id = Column(String, ForeignKey("routes.route_id"), index=True)
    origin_id = Column(String)
    destination_id = Column(String)
    contains_id = Column(String)

    fare = relationship(Fare, backref="rules")
    route = relationship(Route, backref="fare_rules")


class Frequency(Entity, Base):
    __tablename__ = "frequencies"

    inbound_conversions = {'start_time': make_time,
                           'end_time': make_time,
                           'headway_secs': int}

    id = Column(Integer, primary_key=True)
    trip_id = Column(String, ForeignKey("trips.trip_id"),
                     index=True, nullable=False)
    start_time = Column(TransitTimeType, nullable=False)
    end_time = Column(TransitTimeType, nullable=False)
    headway_secs = Column(Integer, nullable=False)

    trip = relationship(Trip, backref="frequencies")

    def __repr__(self):
        return "<Frequency %s-%s %s>" % (self.start_time, self.end_time,
                                         self.headway_secs)


class Transfer(Entity, Base):
    __tablename__ = "transfers"

    inbound_conversions = {'transfer_type': int}

    id = Column(Integer, primary_key=True)
    from_stop_id = Column(String, ForeignKey("stops.stop_id"),
                          index=True, nullable=False)
    to_stop_id = Column(String, ForeignKey("stops.stop_id"),
                        index=True, nullable=False)
    transfer_type = Column(Integer, nullable=False)
    min_transfer_time = Column(String)

    from_stop = relationship(Stop,
                             primaryjoin="Transfer.from_stop_id==Stop.stop_id",
                             backref="transfers_away")
    to_stop = relationship(Stop,
                           primaryjoin="Transfer.to_stop_id==Stop.stop_id",
                           backref="transfers_from")
