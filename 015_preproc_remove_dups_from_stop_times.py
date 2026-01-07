import csv
import json
from csv import excel

import mod_distance as mydst
import mod_loader as myload

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "data/stop_times.txt"
filename_stops = "data/stops.txt"

filename_cleanedstoptimes = "preproc/stop_times_cleaned.txt"

trips = myload.load_trips(filename_trips)
print("Loaded: ", len(trips), "trips")

# stop_times
# trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
hash_is_duplicate = {}


out_row=""

fin = open(filename_stoptimes)
fout = open(filename_cleanedstoptimes, "w")
cntr = 0
cntr_duplicates = 0
cntr_errors = 0

while True:
    line = fin.readline()

    # Test for EOF
    if not line:
        break

    cntr = cntr + 1

    if cntr == 1:
        fout.write(line)
        continue

    if cntr % 1000 == 0:
        print(f'line_no: {cntr} , duplicates: {cntr_duplicates}')

    # if cntr == 3000 :
    #     break

    try:
        row = line.split(",")
        trip_id, stop_id, arrival_time, departure_time, stop_sequence, *rest = row

        route_id=trips[trip_id]["route_id"]
        direction_id = trips[trip_id]["direction_id"]
        if stop_id not in hash_is_duplicate:
            hash_is_duplicate[stop_id] = {}
        if route_id not in hash_is_duplicate[stop_id]:
            hash_is_duplicate[stop_id][route_id] = {}
        if direction_id not in hash_is_duplicate[stop_id][route_id] :
            hash_is_duplicate[stop_id][route_id][direction_id] =  set()
        if arrival_time not in hash_is_duplicate[stop_id][route_id][direction_id] :
            hash_is_duplicate[stop_id][route_id][direction_id].add(arrival_time)
            fout.write(line)
        else:
            cntr_duplicates = cntr_duplicates + 1
    except:
        cntr_errors = cntr_errors + 1
        print(f'Error parsing line_no: {cntr} {line}')

fin.close()
fout.close()

