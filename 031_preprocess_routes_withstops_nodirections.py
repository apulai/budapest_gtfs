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

stop_times = myload.load_stop_times(filename_stoptimes)
print("Loaded: ", len(stop_times), "stop_times")

processed_routes = []
stops_along_routes = {}
for trip_id in trips.keys():
    ri = trips[trip_id]["route_id"]

    if ri in processed_routes:
        continue
    processed_routes.append(ri)

    list_of_stops = []
    for idx, d in enumerate(stop_times):
        if d["trip_id"] == trip_id:
            list_of_stops.append(d["stop_id"])

    rv2 = {}
    #route_short_name, route_long_name, route_type, route_desc,
    rv2["route_short_name"]= routes[ri]["route_short_name"]
    rv2["route_desc"] = routes[ri]["route_desc"]
    rv2["list_of_stops"] = list_of_stops
    print(f' {ri}: {rv2}')

    stops_along_routes[ri] = rv2
    #stops_along_routes[ri] = list_of_stops

print("Generated: ", len(stops_along_routes), "routes with stops")

export_filename='preproc/stops_along_routes_nodirections.json'
with open(export_filename, 'w') as fp:
    print(json.dumps(stops_along_routes, indent=4, ensure_ascii=False), file=fp)

export_filename='preproc/stops_along_routes_nodirections.csv'
with open(export_filename, 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["route_id", "stop_list"])
    for key, value in stops_along_routes.items():
        # writer.writerow([key, value])
        writer.writerow([key, value["list_of_stops"]])

