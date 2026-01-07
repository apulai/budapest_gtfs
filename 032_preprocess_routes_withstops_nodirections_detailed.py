import csv
import json
import mod_distance as mydst
import mod_loader as myload

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "preproc/stop_times_cleaned.txt"
filename_stops = "data/stops.txt"


routes = myload.load_routes(filename_routes)
print("Loaded: ", len(routes), "routes")

trips = myload.load_trips(filename_trips)
print("Loaded: ", len(trips), "trips")

stops = myload.load_stops(filename_stops)
print("Loaded: ", len(stops), "stops")

stop_times = myload.load_stop_times(filename_stoptimes)
print("Loaded: ", len(stop_times), "stop_times")

processed_routes = []
stops_along_routes = {}
for trip_id in trips.keys():
    ri = trips[trip_id]["route_id"]
    di = trips[trip_id]["direction_id"]
    if ri in processed_routes:
        continue
    processed_routes.append( ri )

    list_of_stops = []
    for idx, d in enumerate(stop_times):

        if d["trip_id"] != trip_id:
            continue

        if d["trip_id"] == trip_id:
            rv={}
            rv[ "stop_id"] =d["stop_id"]
            rv["stop_sequence"] = d["stop_sequence"]
            rv[ "stop_name" ]=stops[d["stop_id"]]["stop_name"]
            rv["stop_lat"] = stops[d["stop_id"]]["stop_lat"]
            rv["stop_lon"] = stops[d["stop_id"]]["stop_lon"]
            list_of_stops.append(rv)
    rv2 = {}
    #route_short_name, route_long_name, route_type, route_desc,
    rv2["route_short_name"]= routes[ri]["route_short_name"]
    rv2["route_desc"] = routes[ri]["route_desc"]
    rv2["list_of_stops"] = list_of_stops

    stops_along_routes[ri] = rv2

    print(f' {ri} {rv2}')

print("Generated: ", len(stops_along_routes), "routes with stops including details")

export_filename=('preproc/stops_along_routes_nodirections_detailed.json')
with open(export_filename, 'w') as fp:
    print(json.dumps(stops_along_routes, indent=4, ensure_ascii=False), file=fp)

