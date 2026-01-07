import pandas as pd
import numpy as np
import heapq
from collections import defaultdict

from webcolors import hex_to_rgb_percent

import mod_distance
import mod_loader as myload
import time

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "preproc/stop_times.txt"
filename_stops = "data/stops.txt"
filename_stops_and_nearstops = "preproc/stops_w_nearstops.json"
filename_routes_available_from_stops = "preproc/routes_available_from_stops.json"
filename_stops_along_routes_nodirections = "preproc/stops_along_routes_nodirections_detailed.json"


def str_time_to_min(astr):
    [h, m, s] = astr.split(':')
    return int(h) * 60 + int(m)


def min_to_str_time(nummin):
    hh = int(nummin / 60)
    mm = int(nummin - 60 * hh)
    return f"{hh:02}:{mm:02}"


# General walking speed for cities by internet: 5km/h
# General walking seed by Citiy of London: 4.8km/h
# I think it is 3km/h
def getminutestowalk(dst, walkspeed=3.5):
    # km/h to m / minutes
    walkspeed = walkspeed * 1000 / 60
    return dst / walkspeed

#
def get_neighbours(node):
    neigbours = []
    retdict = {}

    # Where can I get by walking in this stop?
    # This data comes from a preprocessed set
    for s in stop_and_nearstops[node["stop_id"]]["nearest_stops"]:
        dst = int(s["distance"])
        walkmin = getminutestowalk(dst) + 3
        neigbours.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"],
                          "arrival_time_min": walkmin + node["arrival_time_min"],
                          "departure_time_min_x": node["arrival_time_min"],
                          "distance": dst,
                          "instruction": 'walk'})

    # Which trips stop / call at this stop?
    stop_id = node["stop_id"]
    arrival_time_min = node["arrival_time_min"]

    # At some end stations it happens that a train arrives and starts from the same
    # location, this is why I can not just droup duplicated by "route_id" but also need direction
    # Like at H8: trip1 from Ors to Godollo, trip2 from Godollo to Ors, I need to keep all different directions!
    #
    df_megallo_jaratok = pd.merge(
        df_stop_times.query(f'stop_id == "{stop_id}" and arrival_time_min > {arrival_time_min}'), df_trips,
        left_on="trip_id", right_on="trip_id").sort_values(["arrival_time_min"]).drop_duplicates(subset=["route_id","direction_id"], keep='first')

    a = df_stops.query(f'stop_id == "{stop_id}"')["stop_name"]
    stop_name = a.values[0]
    df_megallo_jaratok["stop_name"] = stop_name

    # This is just some cosmetics, adding stop names for debugging
    df_megallo_jaratok2 = pd.merge(df_megallo_jaratok, df_routes[["route_id", "route_short_name"]], left_on="route_id",
                                   right_on="route_id")

    # This will join stop times on trip_id
    # As a results I will have the all the stops again (including previous and next stops)
    df_szomszedok = pd.merge(df_megallo_jaratok2, df_stop_times[
        ["trip_id", "stop_id", "arrival_time_min", "arrival_time", "departure_time", "departure_time_min"]], how="left",
                             left_on="trip_id", right_on="trip_id")
    df_szomszedok.rename(
        columns={"stop_name": "stop_name_x", "arrival_time_y": "arrival_time", "arrival_time_min_y": "arrival_time_min",
                 "stop_id_y": "stop_id", }, inplace=True)

    # Cosmetics again, so I will have the stop names
    df_szomszedok2 = pd.merge(df_szomszedok, df_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], how="left",
                              left_on="stop_id", right_on="stop_id")

    # Important step. I have to drop all stations with earlier arrival time
    # than the arrival time of my starting stop
    indices_to_drop = df_szomszedok2[df_szomszedok2["arrival_time_min_x"] > df_szomszedok2["arrival_time_min"]].index
    df_szomszedok2.drop(indices_to_drop, inplace=True)


    # I do not want to see the starting stop either in the results
    indices_to_drop = df_szomszedok2[df_szomszedok2["stop_id_x"] == df_szomszedok2["stop_id"]].index
    df_szomszedok2.drop(indices_to_drop, inplace=True)

    # I will return only the unique neighbours
    df_szomszedok2.drop_duplicates(["stop_id"], inplace=True)

    # Add the instrcution: it is always take!
    df_szomszedok2["instruction"] = "take"

    retdict = df_szomszedok2[
        ["stop_id", "trip_id", "trip_headsign", "route_short_name", "stop_name", "departure_time_min_x", "arrival_time_min", "instruction", "stop_lon",
         "stop_lat", "route_id"]].to_dict(
        'records')

    return neigbours + retdict


# h: Heuristic function h(n) that estimates cost (minutes) from n to goal
def h(node1_id, node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    dst = mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])
    return getminutestowalk(dst)

# h: Heuristic function hvdst(n) that estimates cost (meters) from n to goal
def hvdst(node1_id, node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    dst = mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])
    return dst

# d: Function d(current, neighbor) that returns edge weight
def d(node1_id, node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    dst = mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])
    return getminutestowalk(dst)


def reconstruct_path(came_from, current_stop_id):
    total_path = [current_stop_id]
    while current_stop_id in came_from:
        current_stop_id = came_from[current_stop_id]
        total_path.insert(0, current_stop_id)
    return total_path


def print_results(total_path, instructions):
    for node_id in total_path:

        node = instructions[node_id]

        if "instruction" not in node.keys():
            print(node)
            continue

        if node["instruction"].startswith("walk"):
            dst = node.get("distance", -1)
            dststr = str(dst)+'m'
            istr = f'{min_to_str_time(node["departure_time_min_x"])} walk {dststr:<61}to  {stops[node["stop_id"]]["stop_name"]:<35} {node["stop_id"]:<10}. Arrive at {min_to_str_time(node["arrival_time_min"])}'
        elif node["instruction"].startswith("take"):
            hsstr = '(>'+node.get("trip_headsign","---")+')'
            istr = f'{min_to_str_time(node["departure_time_min_x"])} take {node["route_short_name"]:>6} {hsstr:<30} {node["route_id"]:>6} {node["trip_id"]:>15} to  {node["stop_name"]:<35} {node["stop_id"]:<10}. Arrive at {min_to_str_time(node["arrival_time_min"])}'
        elif node["instruction"].startswith("start"):
            node["stop_name"] = stops[node["stop_id"]]["stop_name"]
            istr = f'{min_to_str_time(node["arrival_time_min"])} Start {' ':<59} at  {node["stop_name"]:<35} {node["stop_id"]:<10}.'

        elif node["instruction"].startswith("arrive"):
            node["stop_name"] = stops[node["stop_id"]]["stop_name"]
            istr = f'{min_to_str_time(node["departure_time_min_x"])} Arrive at {' ':<55} {node["stop_name"]:<35} {node["stop_id"]:<10} at {min_to_str_time(node["arrival_time_min"])}'
        else:
            istr = f'Should not reach this!'
        print(istr)


def a_star(start_node, goal_node, h, get_neighbours, d):
    """
    A* pathfinding algorithm.

    Args:
        start_node["stop_id"]: Starting stop_id
        goal: Goal stop_id
        h: Heuristic function h(n) that estimates cost from n to goal
        get_neighbours: Function that returns neighbours of a stop_id
        d: Function d(current_node["node_id"], neighbour) that returns edge weight

    Returns:
        Path from start_node["stop_id"] to goal, or None if no path exists
    """
    start_time = time.time()

    goal_id = goal_node["stop_id"]
    # The set of discovered nodes (using a min-heap)
    open_set = []

    # Nodes already evaluated
    closed_set_hash = {}

    # Counter to avoid any possibility of equal values (distances) that would
    # confuse_min heap calucations
    # using a 'counter' this way will also guarantee, that the first inserted
    # value come back as first if the distabce (primary measure) is the same
    counter = 0
    runtime_nb = 0
    numcalls_fn_neighbours = 0

    # Dict for storing instuctions for each node
    instructions = {}
    instructions[start_node["stop_id"]] = start_node
    instructions[goal_node["stop_id"]] = goal_node


    # heapq.heappush(where, tuple of ( distance, actual_value))
    # heapq.heappush(open_set, (h(start_node["stop_id"]), start_node["stop_id"]))
    # heapq.heappush(where, tuple of ( distance, counter - which will never be equal to anything, actual_value))
    timetogoal=h(start_node["stop_id"], goal_node["stop_id"])
    heapq.heappush(open_set, (start_node["arrival_time_min"] + timetogoal , counter,
                              start_node["stop_id"]))
    counter = counter + 1

    # For tracking the path
    came_from = {}

    # Cost from start_node["stop_id"] to each stop_id
    g_score = defaultdict(lambda: float('inf'))
    g_score[start_node["stop_id"]] = 0

    # Estimated total cost from start_node["stop_id"] to goal through each stop_id
    f_score = defaultdict(lambda: float('inf'))
    # f_score[start_node["stop_id"]] = h(start_node["stop_id"])
    f_score[start_node["stop_id"]] = start_node["arrival_time_min"] + timetogoal

    print(f"Start time: {min_to_str_time(start_node["arrival_time_min"])} estimated walking time is {timetogoal:6.2f} min.")
    # Track nodes in open_set for membership testing
    open_set_hash = {start_node["stop_id"]}

    while open_set:
        # Get stop_id with lowest f_score
        # heapq will guarantee this
        # minutes, counter, node
        oset_min_fscore, oset_cntr, current_node_stop_id = heapq.heappop(open_set)
        open_set_hash.remove(current_node_stop_id)
        closed_set_hash[current_node_stop_id] = current_node_stop_id

        if current_node_stop_id == goal_id:
            total_path = reconstruct_path(came_from, current_node_stop_id)
            print(f"Solution found! Touched {len(came_from)} stations. Length of path {len(total_path)}.")
            stop_time = time.time()
            runtime = stop_time - start_time
            print(f"a_star run time:         {runtime:10.5f} seconds.")
            print(f"get_neighbours run time: {runtime_nb:10.5f} seconds. Number of calls: {numcalls_fn_neighbours}\n.")

            return total_path, instructions



        start_time_nb = time.time()
        neighbours = get_neighbours(instructions[current_node_stop_id])
        numcalls_fn_neighbours = numcalls_fn_neighbours + 1
        stop_time_nb = time.time()
        runtime_nb = runtime_nb + stop_time_nb - start_time_nb

        for idx, neighbour in enumerate(neighbours):

            #if closed_set_hash.get(neighbour["stop_id"]) is not None:
            #    #print("closed set hashben")
            #    continue

            # Calculate tentative g_score
            nb_tentative_gscore = neighbour["arrival_time_min"]


            # If g_score is higher than it would be better to walk
            # simply continue
            if nb_tentative_gscore > start_node["arrival_time_min"] + timetogoal:
                 continue

            #DEBUG
            #if neighbour["stop_id"] == "F02578" or neighbour["stop_id"] == "F02597" or neighbour["stop_id"] =='F01083' :
            #    print(neighbour)
            # try:
            #     if neighbour["route_id"] == "3060":
            #         print(neighbour)
            # except:
            #     pass

            nb_current_gscore = g_score[neighbour["stop_id"]]

            if nb_tentative_gscore < nb_current_gscore:
                # This path is better than any previous one
                came_from[neighbour["stop_id"]] = current_node_stop_id
                # g actual cost to get here
                g_score[neighbour["stop_id"]] = nb_tentative_gscore
                # f = g + h
                f_score[neighbour["stop_id"]] = nb_tentative_gscore + h(neighbour["stop_id"], goal_id)

                # Need to update how to get to this point, since gscore is bettter
                instructions[neighbour["stop_id"]] = neighbour

                debug = 0
                if debug == 1:
                    print(f'{current_node_stop_id:6} {instructions[current_node_stop_id]["stop_name"]:30} to {neighbour["stop_id"]:6} {neighbour["stop_name"]:30} {neighbour["instruction"]:5}',end="")
                    try:
                        print(f'{neighbour["route_short_name"]:5}',end="")
                    except:
                        print('     ', end="")
                    print(f' G_curr: {g_score[instructions[current_node_stop_id]["stop_id"]]:8.3f} G_nb: {nb_tentative_gscore:8.3f}    F_curr: {f_score[instructions[current_node_stop_id]["stop_id"]]:8.3f} {instructions[current_node_stop_id]["stop_id"]:6}  F_nb: {f_score[neighbour["stop_id"]]:8.3f} total_dst:{hvdst(neighbour["stop_id"],goal_id):8.2f}')

                if neighbour["stop_id"] not in open_set_hash:

                    heapq.heappush(open_set, (f_score[neighbour["stop_id"]], counter, neighbour["stop_id"]))
                    counter = counter + 1
                    open_set_hash.add(neighbour["stop_id"])
                    # heapq experienced errors if 2 values was present with the same key (distance)
                    # try:
                    #     heapq.heappush(open_set, (f_score[neighbour["stop_id"]], neighbour))
                    #     open_set_hash.add(neighbour["stop_id"])
                    # except:
                    #     print(f'Hiba:  {f_score[neighbour["stop_id"]]} {neighbour["stop_id"]} {stops[neighbour["stop_id"]]["stop_name"]:<30} ')
                # else:
                #     for idx,elem in enumerate(open_set):
                #         fscore, cnt, stop_id = elem
                #         if stop_id == neighbour["stop_id"]:
                #             open_set[idx] = open_set[-1]
                #             open_set.pop()
                #             heapq.heapify(open_set)
                #             break
                #     heapq.heappush(open_set, (f_score[neighbour["stop_id"]], counter, neighbour["stop_id"]))
                #     counter = counter + 1


    # No path found
    print("No path found")

    stop_time = time.time()
    runtime = stop_time - start_time
    print(f"a_star run time:         {runtime:10.5f} seconds.")
    print(f"get_neighbours run time: {runtime_nb:10.5f} seconds. Number of calls: {numcalls_fn_neighbours}\n.")

    return None


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

stops_along_routes_nodirections = myload.load_stops_along_routes_nodirections(filename_stops_along_routes_nodirections)
print("Loaded: ", len(stops_along_routes_nodirections), "stops_along_routes")

df_trips = pd.read_csv("data/trips.txt")
print("Loaded trips.txt to pandas df")

df_routes = pd.read_csv("data/routes.txt")
print("Loaded routes.txt to pandas df")

df_stops = pd.read_csv("data/stops.txt")
print("Loaded stops.txt to pandas df")

# df_stop_times = pd.read_csv("data/stop_times.txt")
# print("Loaded stop_times.txt to pandas df")
#
# df_stop_times["arrival_time_min"] = df_stop_times["arrival_time"].apply(str_time_to_min)
# print("Converted arrival times from string to int")
# df_stop_times["departure_time_min"] = df_stop_times["departure_time"].apply(str_time_to_min)
# print("Converted departure times from string to int")
#
# df_stop_times.to_csv('preproc/stop_times_with_min.csv', encoding='utf-8')
# quit()

# df_pathways = pd.read_csv("data/pathways.txt")
# print("Loaded: ", len(stops_along_routes_nodirections), "stops_along_routes")

df_stop_times = pd.read_csv("preproc/stop_times_with_min.csv")
print("Loaded stop_times.txt to pandas df")

# arrival_time_min = str_time_to_min("09:02:00")
# nF00191 = {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min}
# nF00189 = {"stop_id": "F00189", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 2}
# nF01081 = {"stop_id": "F01081", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 5}
# rvF00191 = get_neighbours(nF00191)
# rvF00189 = get_neighbours(nF00189)
# rvF00181 = get_neighbours(nF01081)

arrival_time_min = str_time_to_min("09:02:00")
nF01749 = {"stop_id": "F01749", "instruction": 'start', 'arrival_time_min': arrival_time_min}
n19795278 = {"stop_id": "19795278", "instruction": 'start', 'arrival_time_min': arrival_time_min + 2}
n19795279 = {"stop_id": "19795279", "instruction": 'start', 'arrival_time_min': arrival_time_min + 5}

rvF01749 = get_neighbours(nF01749)
rv19795278 = get_neighbours(n19795278)
rv19795279 = get_neighbours(n19795279)

test_journeys = [
    {
        "start_node": {"stop_id": "F01878", "instruction": 'start', 'arrival_time_min': arrival_time_min,
                       "stop_name": "Pöttyös utca"},
        "goal_node": {"stop_id": "F00956", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600,
                      "stop_name": "Arany János utca"}
    },
    {
        "start_node": {"stop_id": "008077", "instruction": 'start', 'arrival_time_min': arrival_time_min,
                       "stop_name": "Pöttyös utca"},
        "goal_node": {"stop_id": "F00943", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600,
                      "stop_name": "Arany János utca"}
    },
    {
        "start_node": {"stop_id": "F01755", "instruction": 'start', 'arrival_time_min': arrival_time_min,
                       "stop_name": "Örs vezér tere M+H"},
        "goal_node": {"stop_id": "F00897", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600,
                      "stop_name": "Újpest-városkapu"}
    },
    {
        # "start_stop": "F00191", # Margit híd, budai hídfő H
        # VIA "stop_id": "F00189", "Margit híd, budai hídfő H",
        # VIA "stop_id": "F01081", "stop_name": "Oktogon M",
        # "final_stop": "F01083" # Oktogon
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "008152", "instruction": 'start', 'arrival_time_min': arrival_time_min+4},
        "goal_node": {"stop_id": "008163", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F01755", # Örs vezér tere M+H'
        # "final_stop": "F01083" #  Oktogon M
        "start_node": {"stop_id": "F01755", "instruction": 'start', 'arrival_time_min': arrival_time_min+4},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F02268", # KFKI
        # "final_stop": "19868321" # Gödöllő, Szabadság tér
        "start_node": {"stop_id": "F02268", "instruction": 'start', 'arrival_time_min': arrival_time_min},
        "goal_node": {"stop_id": "19868321", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F02268", # KFKI
        # "final_stop": "19868321" # Gödöllő, Szabadság tér
        "start_node": {"stop_id": "F02268", "instruction": 'start', 'arrival_time_min': 420},
        "goal_node": {"stop_id": "19868321", "instruction": 'arrive', 'arrival_time_min': 420 + 120}
    },
    {
        # "start_stop": "F01749", # Örs (601)
        # "final_stop": "19868321" # Gödöllő, Szabadság tér
        "start_node": {"stop_id": "F01749", "instruction": 'start', 'arrival_time_min': 541},
        "goal_node": {"stop_id": "19868321", "instruction": 'arrive', 'arrival_time_min': 640}
    },
    {
        # "start_stop": "F01749", # Örs (601)
        # "final_stop": "19868321" # Gödöllő, Szabadság tér
        "start_node": {"stop_id": "F01749", "instruction": 'start', 'arrival_time_min': 601},
        "goal_node": {"stop_id": "19868321", "instruction": 'arrive', 'arrival_time_min': 700}
    },
    {
        # "start_stop": "19868321" # Gödöllő, Szabadság tér
        # "final_stop": "F02268", # KFKI
        "start_node": {"stop_id": "19868321", "instruction": 'start', 'arrival_time_min': arrival_time_min + 600},
        "goal_node": {"stop_id": "F02268", "instruction": 'arrive', 'arrival_time_min': arrival_time_min}

    },

    {
        # "start_stop": "F02268", # KFKI
        # "final_stop": "009019" # Gödöllő, Palotakert
        "start_node": {"stop_id": "F02268", "instruction": 'start', 'arrival_time_min': arrival_time_min},
        "goal_node": {"stop_id": "19869324", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F02268", # KFKI
        # "final_stop": "009019" # Gödöllő, Palotakert
        "start_node": {"stop_id": "F02268", "instruction": 'start', 'arrival_time_min': arrival_time_min},
        "goal_node": {"stop_id": "009019", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    }
]

for journey in test_journeys:
    print()
    print("---")
    start_node = journey["start_node"]
    start_node["stop_name"] = stops[start_node["stop_id"]]["stop_name"]

    goal_node = journey["goal_node"]
    goal_node["stop_name"] = stops[goal_node["stop_id"]]["stop_name"]

    print(f'{start_node["stop_id"]} {start_node["stop_name"]} --> {goal_node["stop_id"]} {goal_node["stop_name"]}')

    total_path, instructions = a_star(start_node, goal_node, h, get_neighbours, h)

    print_results(total_path, instructions)



print()

arrival_time_min1 = str_time_to_min("05:17:00")
arrival_time_min2 = str_time_to_min("09:02:00")
arrival_time_min3 = str_time_to_min("15:50:00")
arrival_time_min4 = str_time_to_min("22:40:00")
arrival_time_min5 = str_time_to_min("23:15:00")

test_journeys = [
    {
        # "start_stop": "F00191", # Margit híd, budai hídfő H
        # VIA "stop_id": "F00189", "Margit híd, budai hídfő H",
        # VIA "stop_id": "F01081", "stop_name": "Oktogon M",
        # "final_stop": "F01083" # Oktogon
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min5,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min5 + 60, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min4,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min4 + 60, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min3,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min3 + 60, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min2,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min2 + 60, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min1,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min1 + 60, "stop_name": "end_oktogon"}
    }
]

for journey in test_journeys:
    print()
    print("---")
    start_node = journey["start_node"]
    start_node["stop_name"] = stops[start_node["stop_id"]]["stop_name"]

    goal_node = journey["goal_node"]
    goal_node["stop_name"] = stops[goal_node["stop_id"]]["stop_name"]

    print(f'{start_node["stop_id"]} {start_node["stop_name"]} --> {goal_node["stop_id"]} {goal_node["stop_name"]}')

    total_path, instructions = a_star(start_node, goal_node, h, get_neighbours, h)

    print_results(total_path, instructions)



print()

#arrival_time_min = str_time_to_min("05:15:00")
arrival_time_min = str_time_to_min("09:02:00")
#arrival_time_min = str_time_to_min("15:50:00")
#arrival_time_min = str_time_to_min("22:40:00")
#arrival_time_min = str_time_to_min("23:15:00")


