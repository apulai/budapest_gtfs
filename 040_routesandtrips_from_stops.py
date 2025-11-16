import csv
import json
import mod_distance as mydst
import mod_loader as myload

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "data/stop_times.txt"
filename_stops = "data/stops.txt"


routes = myload.load_routes(filename_routes)
print("Loaded: ", len(routes), "routes")

trips = myload.load_trips(filename_trips)
print("Loaded: ", len(trips), "trips")

stops = myload.load_stops(filename_stops)
print("Loaded: ", len(stops), "stops")

stop_times = myload.load_stop_times(filename_stoptimes)
print("Loaded: ", len(stop_times), "stop_times")

routes_available_from_stops = {}

#trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
cntr = 0
for e in stop_times:
    cntr = cntr + 1
    stop_id = e["stop_id"]
    trip_id = e["trip_id"]
    arrival_time = e["arrival_time"]
    stop_sequence = e["stop_sequence"]
    route_id = trips[trip_id]["route_id"]

    if stop_id not in routes_available_from_stops.keys():
        routes_available_from_stops[stop_id] = {}
        routes_available_from_stops[stop_id]["routes"]=[]
        routes_available_from_stops[stop_id]["trips"]=[]

    if route_id not in routes_available_from_stops[stop_id]["routes"]:
        routes_available_from_stops[stop_id]["routes"].append(route_id)

    if trip_id not in routes_available_from_stops[stop_id]["trips"]:
        routes_available_from_stops[stop_id]["trips"].append(trip_id)

    if cntr % 1000 == 0:
        print(cntr)

export_filename=('preproc/routes_available_from_stops.json')
with open(export_filename, 'w') as fp:
    print(json.dumps(routes_available_from_stops, indent=4, ensure_ascii=False), file=fp)


