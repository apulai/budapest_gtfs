import mod_loader as myload

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "data/stop_times.txt"
filename_stops = "data/stops.txt"
filename_stops_and_nearstops = "preproc/stops_w_nearstops.json"
filename_routes_available_from_stops = "preproc/routes_available_from_stops.json"

routes = myload.load_routes(filename_routes)
print("Loaded: ", len(routes), "routes")

trips = myload.load_trips(filename_trips)
print("Loaded: ", len(trips), "trips")

stops = myload.load_stops(filename_stops)
print("Loaded: ", len(stops), "stops")

routes_available_from_stops = myload.load_routes_available_from_stops(filename_routes_available_from_stops)
print("Loaded: ", len(routes_available_from_stops), "route connections")

stop_and_nearstops = myload.load_stop_and_nearstops(filename_stops_and_nearstops)
print("Loaded: ", len(stops), "walkable changes")

stops_along_routes_nodirections =