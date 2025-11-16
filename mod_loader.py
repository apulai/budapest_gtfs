import csv
import json

def load_stops(filename):
    dict_stops = {}
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='"')
        cntr = 0
        for row in csvreader:
            cntr = cntr + 1
            if cntr == 1:
                headers = row
                continue
            #print(row)
            (stop_id,stop_name,stop_lat,stop_lon,stop_code,location_type,location_sub_type,parent_station,wheelchair_boarding) = row
            dict_stops[stop_id]={
                "stop_name": stop_name,
                "stop_lat": float (stop_lat),
                "stop_lon": float (stop_lon),
                #"nearest_stops": []
            }
    return dict_stops

# routes
# agency_id,route_id,route_short_name,route_long_name,route_type,route_desc,route_color,route_text_color,route_sort_order
def load_routes(filename):
    dict_routes = {}
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='"')
        cntr = 0
        for row in csvreader:
            cntr = cntr + 1
            if cntr == 1:
                headers = row
                continue
            #print(row)
            (agency_id,route_id,route_short_name,route_long_name,route_type,route_desc,route_color,route_text_color,route_sort_order) = row
            dict_routes[route_id]={
                "route_short_name": route_short_name,
                #"route_long_name": route_long_name,
                "route_type": int (route_type),
                "route_desc": route_desc
            }
    return dict_routes

# trips
#route_id,trip_id,service_id,trip_headsign,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed
def load_trips(filename):
    dict_trips = {}
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='"')
        cntr = 0
        for row in csvreader:
            cntr = cntr + 1
            if cntr == 1:
                headers = row
                continue
            #print(row)
            (route_id,trip_id,service_id,trip_headsign,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed) = row
            dict_trips[trip_id]={
                "route_id": route_id,
                "service_id": service_id,
                "trip_headsign": trip_headsign,
                "direction_id": direction_id,
                "block_id": block_id,
                "shape_id": shape_id
            }
    return dict_trips

# stop_times
# trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
def load_stop_times(filename):
    list_stop_times = []
    with open(filename) as f:
        csvreader = csv.reader(f, delimiter=',', quotechar='"')
        cntr = 0
        for row in csvreader:
            cntr = cntr + 1
            if cntr == 1:
                headers = row
                continue
            #print(row)
            (trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled) = row
            d = {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "arrival_time": arrival_time,
                "departure_time": departure_time,
                "stop_sequence": stop_sequence
            }
            list_stop_times.append(d)

    return list_stop_times


# routes_available_from_stops
def load_routes_available_from_stops(filename):
    with open(filename) as f:
            routes_available_from_stops = json.load(f)
    return routes_available_from_stops

# routes_available_from_stops
def load_stop_and_nearstops(filename):
    with open(filename) as f:
            stop_and_neartstops = json.load(f)
    return stop_and_neartstops

# stops_along_routes_nodirections
def load_stops_along_routes_nodirections(filename):
    with open(filename) as f:
            stops_along_routes_nodirections = json.load(f)
    return stops_along_routes_nodirections
