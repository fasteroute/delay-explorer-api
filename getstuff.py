import datetime
import json
import os
import psycopg2
import uuid

from collections import defaultdict
from random import uniform

from connection import Connection

from locations import CrsMapper

def dow_from_int(i):
    if i == 0:
        return "mon"
    elif i == 1:
        return "tue"
    elif i == 2:
        return "wed"
    elif i == 3:
        return "thu"
    elif i == 4:
        return "fri"
    elif i == 5:
        return "sat"
    elif i == 6:
        return "sun"
    else:
        raise Exception("Unrecognised day of week integer {}".format(i))

class TrainLateness:
    def __init__(self, day, average_lateness, histogram=None):
        self.day = day
        self.average_lateness = average_lateness
        if histogram is None:
            self.histogram = []
        else:
            self.histogram = histogram

class HistogramItem:
    def __init__(self, percent, period):
        self.percent = percent
        self.period = period

class Route:
    """
    id
    stations []
    """
    def __init__(self, id, stations=None):
        self.id = id
        if stations is None:
            self.stations = []
        else:
            self.stations = stations

    def __repr__(self):
        return "{}: {}".format(self.id, self.stations)

class Segment:
    """
    id
    origin
    destination
    lateness
    routes []
    """
    def __init__(self, id, origin, destination, lateness=None, routes=None):
        self.id = id
        self.origin = origin
        self.destination = destination
        self.lateness = lateness
        if routes is None:
            self.routes = []
        else:
            self.routes = routes

class Station:
    """
    id
    name
    category
    lat
    lon
    """
    def __init__(self, id, name, category, lat, lon):
        self.id = id
        self.name = name
        self.category = category
        self.lat = lat
        self.lon = lon

class Train:
    """
    id
    route
    name
    lateness []
    """
    def __init__(self, id, route, name, period, lateness=None):
        self.id = id
        self.route = route
        self.name = name
        self.period = period
        if lateness is None:
            self.lateness = []
        else:
            self.lateness=lateness


class LatenessBuilder:
    def __init__(self, cursor, slid, use_departure):
        self.cursor = cursor
        self.slid = slid
        self.ud = use_departure

    def build(self):
        # Initialise data storage
        self.data = []
        for i in range(0, 7):
            self.data.append({
                    "samples": 0,
                    "repsamples": 0,
                    "1": [],
                    "5": [],
                    "15": [],
                    "more": [],
                    "can": [],
                    "norep": []
            })

        # Run the query to get the data that will populate this.
        self.cursor.execute("select date, lateness_arriving, lateness_departing, cancelled from pro_lateness where pro_schedule_location_id=%s",
                (self.slid,))
        rows = self.cursor.fetchall()
        for r in rows:
            if self.ud:
                t = r[2]
            else:
                t = r[1]
            #print(r)
            if t is None:
                b = "norep"
                self.data[r[0].weekday()][b].append(0)
            else:
                seconds = (t.days * 86400) + t.seconds
                if r[3] is True:
                    b = "can"
                elif seconds <= 0:
                    b = "1"
                elif seconds <= 300:
                    b = "5"
                elif seconds <= 1500:
                    b = "15"
                elif seconds > 1500:
                    b = "more"
                else:
                    b = "norep"
                
                self.data[r[0].weekday()][b].append(seconds)
                self.data[r[0].weekday()]["repsamples"] += 1

            self.data[r[0].weekday()]["samples"] += 1

        lateness = []
        #print(self.data)
        for i in range(0, 7):
            d = self.data[i]
            if d["samples"] == 0:
                continue

            accumulator = 0
            for j in d["1"]:
                accumulator += j
            for j in d["5"]:
                accumulator += j
            for j in d["15"]:
                accumulator += j
            for j in d["more"]:
                accumulator += min(j, 30*60)
            for j in d["can"]:
                accumulator += 30*60
 
            samples = d["samples"]
            if samples == 0:
                samples = 1
            repsamples = d["repsamples"]
            if repsamples == 0:
                repsamples = 1

            lateness.append(TrainLateness(dow_from_int(i), (float(accumulator)/repsamples)/60, [
                    HistogramItem((float(len(d["1"]))/samples)*100, "On time"),
                    HistogramItem((float(len(d["5"]))/samples)*100, "5 mins late"),
                    HistogramItem((float(len(d["15"]))/samples)*100, "15 mins late"),
                    HistogramItem((float(len(d["more"]))/samples)*100, ">15 mins late"),
                    HistogramItem((float(len(d["can"]))/samples)*100, "Cancelled"),
                    HistogramItem((float(len(d["norep"]))/samples)*100, "Unknown"),
                    ]))

        #print(lateness)
        return lateness


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if type(o) is Route:
            return {
                    "id": str(o.id),
                    "stations": [station for station in o.stations]
                    }
        if type(o) is Train:
            return  {
                    "id": o.id,
                    "name": o.name,
                    "route": str(o.route),
                    "lateness": o.lateness,
                    "period": o.period
                    }
        if type(o) is Segment:
            return  {
                    "routes": [str(route) for route in o.routes],
                    "lateness": o.lateness,
                    "origin": o.origin,
                    "destination": o.destination,
                    "id": o.id
                    }
        return o.__dict__

# IDIOT ALERT. This code belongs where the actual lateness is calculated - in process.py... :'(
def calculate_lateness(st, at):
    if at is None or st is None:
        return None
    else:
        dif = at - st
        if dif < datetime.timedelta(seconds=-12*60*60):
            # Arrived more than 12 hours early. This means delayed across midnight.
            return datetime.timedelta(seconds=dif.seconds)
        if dif < datetime.timedelta(seconds=0):
            # Arrived up to 12 hours early. Return 0 for lateness.
            return 0
        if dif < datetime.timedelta(seconds=12*60*60):
            # Arrived up to 12 hours late. Return the lateness.
            return dif
        # Arrived more than 12 hours late. This means arrived early across midnight.
        return 0

def get_lateness_value_on_arrival(cursor, psl_id):
    cursor.execute("SELECT lateness_arriving, cancelled from pro_lateness where pro_schedule_location_id=%s", (psl_id,))

    l = []
    for r in cursor.fetchall():
        if r[1] is True:
            l.append(30)
        elif r[0] is not None:
            l.append((r[0].days * 60*60*24) + r[0].seconds)

    if len(l) == 0:
        return None
    
    acc = sum(l)
    return float(acc)/len(l)


class Querier:
    def setup(self):
        self.connection = Connection(host=os.environ["POSTGRES_HOST"],
                            dbname=os.environ["POSTGRES_DB"],
                            user=os.environ["POSTGRES_USER"],
                            password=os.environ["POSTGRES_PASS"])

        self.connection.connect()

        self.cursor = self.connection.cursor()

        self.cm = CrsMapper("locations.json")

    def get_some_data(self, origin, destination, _type,  _time):
        connection = self.connection
        cursor = self.cursor
        d=(datetime.datetime.now() - datetime.timedelta(days=9)).date()

        # Set the query up appropriately for the data we have received.
        if origin is not None and destination is not None:
            cursor.execute("select ps.id, ps.uid, psl1.id, psl2.id from pro_schedule ps INNER JOIN pro_schedule_location psl1 on (ps.id=psl1.pro_schedule_id and psl1.crs=%s and psl1.scheduled_departure_time >= %s) INNER JOIN pro_schedule_location psl2 on (ps.id=psl2.pro_schedule_id and psl2.crs=%s and psl1.scheduled_departure_time < psl2.scheduled_arrival_time) where ps.{} > %s ORDER BY psl1.scheduled_departure_time ASC LIMIT 10".format("weekday" if _type == "weekdays" else "weekend"), (origin, _time, destination, d))
        elif origin is not None and destination is None:
            cursor.execute("select ps.id, ps.uid, psl1.id from pro_schedule ps INNER JOIN pro_schedule_location psl1 on (ps.id=psl1.pro_schedule_id and psl1.crs=%s and psl1.scheduled_departure_time >= %s) where ps.{} > %s ORDER BY psl1.scheduled_departure_time ASC LIMIT 10".format("weekday" if _type == "weekdays" else "weekend"), (origin, _time, d))
        else:
            cursor.execute("select ps.id, ps.uid, psl1.id from pro_schedule ps INNER JOIN pro_schedule_location psl1 on (ps.id=psl1.pro_schedule_id and psl1.crs=%s and psl1.scheduled_arrival_time >= %s) where ps.{} > %s ORDER BY psl1.scheduled_arrival_time ASC LIMIT 10".format("weekday" if _type == "weekdays" else "weekend"), (destination, _time, d))

        rows = cursor.fetchall()

        routes = []
        segments = []
        trains = []
        stations = []

        for row in rows:
            cursor.execute("SELECT id, pro_schedule_id, crs, scheduled_arrival_time, scheduled_departure_time, type, position, date_last_seen from pro_schedule_location where pro_schedule_id=%s ORDER BY position ASC", (row[0],))

            rs = cursor.fetchall()
            previous = None
            route = Route(id=uuid.uuid4())
            passed_journey_destination = False
            dt = None
            orr = None
            departure_time = None
            arrival_time = None
            slid_for_train_lateness = None
            for r in rs:
                if orr is None:
                    if r[5] == 'OR':
                        orr = r[2]
                if passed_journey_destination is True:
                    if r[5] == 'DT':
                        dt = r[2]
                    continue

                # We haven't reached the origin yet. Iterate until we get there.
                if previous is None:
                    # If this is not the origin, continue to the next iteration.
                    if r[0] != row[2] and origin is not None:
                        continue
                    # It's the origin.
                    stations.append(Station(r[2], self.cm.name(r[2]), 1, self.cm.lat(r[2]), self.cm.lon(r[2])))
                    route.stations.append(r[2])
                    previous = r
                    departure_time = r[4]
                    #if destination is None:
                    #    slid_for_train_lateness = r[0]
                    continue

                # Create a station for where we've arrived, and create a segment from previous to here.
                stations.append(Station(r[2], self.cm.name(r[2]), 1, self.cm.lat(r[2]), self.cm.lon(r[2])))
                route.stations.append(r[2])
                seglate = get_lateness_value_on_arrival(self.connection.cursor(), r[0])
                segments.append(Segment("{}{}".format(previous[2],r[2]), previous[2], r[2], seglate, [route.id,]))
                previous = r

                # If we've reached the destination. Don't break out of the loop though cos we need
                # to know the train's final destination.
                if destination is not None and origin is not None:
                    if r[0] == row[3]:
                        slid_for_train_lateness = r[0]
                        passed_journey_destination = True
                        if r[5] == 'DT':
                            dt = r[2]
                        continue
                else:
                    if origin is None and arrival_time is None:
                        if r[0] == row[2]:
                            slid_for_train_lateness = r[0]
                            arrival_time = r[3]
                            passed_journey_destination = True
                    dt = r[2]
                    # Temporary hack
                    slid_for_train_lateness = r[0]

            # Add the route to the list.
            routes.append(route)

            #print(slid_for_train_lateness)
            if destination is None and origin is not None:
                #use_departure=True
                use_departure=False
            else:
                use_departure=False
            lb = LatenessBuilder(self.connection.cursor(), slid_for_train_lateness, use_departure=use_departure)
            lateness = lb.build()
            
            if origin is not None:
                train_name = "{} to {}".format(departure_time.strftime("%H:%M"), self.cm.name(dt))
            else:
                train_name = "{} from {}".format(arrival_time.strftime("%H:%M"), self.cm.name(orr))
            trains.append(Train(row[1], route.id, train_name, "Data collected over last 4 weeks", lateness))


        # Deduplicate routes, segments, and stations. Trains don't need it as they are queried uniquely from DB.
        new_stations = {}
        for i in stations:
            if i.id not in new_stations:
                new_stations[i.id] = i
        stations = list(new_stations.values())

        new_segments = {}
        for i in segments:
            if i.id not in new_segments:
                i.l_acc = []
                if i.lateness is not None:
                    i.l_acc.append(i.lateness)
                new_segments[i.id] = i
            else:
                [new_segments[i.id].routes.append(a) for a in i.routes]
                if i.lateness is not None:
                    new_segments[i.id].l_acc.append(i.lateness)
        segments = list(new_segments.values())

        # Collapse new segment latenesses.
        for _,v in new_segments.items():
            if len(v.l_acc) == 0:
                v.lateness == None
            else:
                acc = sum(v.l_acc)
                v.lateness = (float(acc)/len(v.l_acc))/60

        new_routes = {}
        for i in routes:
            key = "-".join(i.stations)
            if key not in new_routes:
                new_routes[key] = i
            else:
                for j in segments:
                    if i.id in j.routes:
                        j.routes.remove(i.id)
                        if new_routes[key].id not in j.routes:
                            j.routes.append(new_routes[key].id)
                for k in trains:
                    if i.id == k.route:
                        k.route = new_routes[key].id
        routes = list(new_routes.values())

        # Turn it into JSON.
        j = {}
        j["routes"] = routes
        j["segments"] = segments
        j["trains"] = trains
        j["stations"] = stations
        if origin is not None:
            j["origin_text"] = self.cm.name(origin)+" ["+origin+"]"
        if destination is not None:
            j["destination_text"] = self.cm.name(destination)+" ["+destination+"]"

        return json.dumps(j, cls=JsonEncoder, indent=4)

    def get_calling_points(self, origin, destination, uid):
        cursor = self.cursor
        cursor.execute("select pro_schedule_location.id, crs, scheduled_arrival_time, scheduled_departure_time, type from pro_schedule, pro_schedule_location where uid=%s and pro_schedule.id=pro_schedule_location.pro_schedule_id ORDER BY position ASC", (uid,))

        rows = cursor.fetchall()

        points = []
        reached_origin = False if origin is not None else True
        reached_destination = False
        for row in rows:
            if reached_origin is False and row[1] != origin:
                continue

            reached_origin = True

            lb = LatenessBuilder(self.connection.cursor(), row[0], use_departure=True if row[4] == 'OR' or row[1] == origin else False)
            lateness = lb.build()
            
            data = {
                "name": self.cm.name(row[1]),
                "id": row[0],
                "time": row[2].strftime("%H:%M") if row[2] is not None else row[3].strftime("%H:%M"),
                "lateness": lateness,
            }

            points.append(data)

            # If we've reached the destination, break out of the loop.
            if destination != None and row[1] == destination:
                break

        data = {
                uid: {
                    "callingPoints": points
                    }
                }
        return json.dumps(data, cls=JsonEncoder, indent=4)


if __name__ == "__main__":
    q = Querier()
    q.setup()
    print(q.get_some_data(
        origin=None,
        destination="GTW",
        _type="weekend",
        _time="11:00"
    ))

