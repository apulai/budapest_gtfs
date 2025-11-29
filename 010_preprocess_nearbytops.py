filename = "data/stops.txt"

import csv
import json
import mod_distance as mydst
import mod_loader as myload

maxdst = 200

d = mydst.equirectangular_distance(47.509806,19.057151,47.509922,19.056658)
print(d)
d = mydst.haversine_distance(47.509806,19.057151,47.509922,19.056658)
print(d)

d = mydst.equirectangular_distance(47.509806,19.057151,47.510326,19.056196)
print(d)
d = mydst.haversine_distance(47.509806,19.057151,47.510326,19.056196)
print(d)

def nearest_stops_detailed_json(dict_stops, maxdst = 100, export_filename = "data/stops_w_nearstops.json"):
    close_pairs = 0
    idx = 0
    for stop_id1 in dict_stops.keys():
        idx = idx + 1
        #print(idx)
        cnt = 0
        for stop_id2 in dict_stops.keys():
            if stop_id1 == stop_id2:
                continue
            lon1 = dict_stops[stop_id1]["stop_lon"]
            lat1 = dict_stops[stop_id1]["stop_lat"]
            lon2 = dict_stops[stop_id2]["stop_lon"]
            lat2 = dict_stops[stop_id2]["stop_lat"]
            dst = mydst.haversine_distance(lat1, lon1, lat2, lon2)
            if (dst < maxdst):
                cnt = cnt + 1
                close_pairs = close_pairs + 1
                res = {}
                res["stop_id"] = stop_id2
                res["stop_name"] = dict_stops[stop_id2]["stop_name"]
                res["distance"] = dst
                dict_stops[stop_id1]["nearest_stops"].append(res)
                # print(dict_stops[stop_id1]["stop_name"]," --- ",  dict_stops[stop_id2]["stop_name"],dst)
        print(idx, stop_id1, dict_stops[stop_id1]["stop_name"], cnt)
    print("Number of close stop pairs:",close_pairs)

    with open(export_filename, 'w') as fp:
        print( json.dumps(dict_stops, indent = 4, ensure_ascii=False), file= fp )


def nearest_stops_short_csv(dict_stops, maxdst=100, export_filename="data/stops_w_nearstops.csv"):
    close_pairs = 0
    idx = 0
    for stop_id1 in dict_stops.keys():
        idx = idx + 1
        #print(idx)
        cnt = 0
        for stop_id2 in dict_stops.keys():
            if stop_id1 == stop_id2:
                continue
            lon1 = dict_stops[stop_id1]["stop_lon"]
            lat1 = dict_stops[stop_id1]["stop_lat"]
            lon2 = dict_stops[stop_id2]["stop_lon"]
            lat2 = dict_stops[stop_id2]["stop_lat"]
            dst = mydst.haversine_distance(lat1, lon1, lat2, lon2)
            if (dst < maxdst):
                cnt = cnt + 1
                close_pairs = close_pairs + 1
                dict_stops[stop_id1]["nearest_stops"].append(stop_id2)
                # print(dict_stops[stop_id1]["stop_name"]," --- ",  dict_stops[stop_id2]["stop_name"],dst)
        print(idx, stop_id1, dict_stops[stop_id1]["stop_name"], cnt)
    print("Number of close stop pairs:",close_pairs)

    with open(export_filename, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["stop_id", "stop_name","stop_lat","stop_lon","nearest_stops"])
        for key, value in dict_stops.items():
            #writer.writerow([key, value])
            writer.writerow([key, dict_stops[key]["stop_name"], dict_stops[key]["stop_lat"], dict_stops[key]["stop_lon"], dict_stops[key]["nearest_stops"], ])


dict_stops = myload.load_stops(filename)
print(f'loaded {len(dict_stops)} stops')
for stop_id in dict_stops.keys():
    dict_stops[stop_id]["nearest_stops"]=[]
nearest_stops_detailed_json(dict_stops, maxdst , export_filename = "preproc/stops_w_nearstops.json")
print(f'Export done to json')

dict_stops = myload.load_stops(filename)
for stop_id in dict_stops.keys():
    dict_stops[stop_id]["nearest_stops"]=[]
print(f'loaded {len(dict_stops)} stops')
nearest_stops_short_csv(dict_stops, maxdst , export_filename="preproc/stops_w_nearstops.csv")
print(f'Export done to json')


