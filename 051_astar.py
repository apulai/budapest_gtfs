import pandas as pd
import numpy as np
import heapq
from collections import defaultdict
import mod_distance
import mod_loader as myload

filename_routes = "data/routes.txt"
filename_trips = "data/trips.txt"
filename_stoptimes = "data/stop_times.txt"
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


def get_neighbours(stop_id):
    neigbours = []
    routing = []

    # Ahova at lehet sétálni ebben a megállóban
    for s in stop_and_nearstops[stop_id]["nearest_stops"]:
        dst = int(s["distance"])
        neigbours.append({"stop_id": s["stop_id"], "distance": dst, "instruction": 'walk'})

    # Amelyik vonalak megállnak ebben a megállóban azoknak az állomásai
    if routes_available_from_stops.get(stop_id) != None:
        for r in routes_available_from_stops[stop_id]["routes"]:
            # Kizártam az éjszakai járatokat, mert azokat nem ismerem
            # Meg valamiért előszerettel választja az algoritmus
            if r.isnumeric() and int(r) > 8999:
                continue
            for s in stops_along_routes_nodirections[r]["list_of_stops"]:
                neigbours.append({"stop_id": s["stop_id"], "route": r, "instruction": "take"})

    return neigbours


def get_neighbours(node):
    neigbours = []
    retdict = {}

    # Ahova at lehet sétálni ebben a megállóban
    for s in stop_and_nearstops[node["stop_id"]]["nearest_stops"]:
        dst = int(s["distance"])
        walkmin = getminutestowalk(dst)
        neigbours.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"],
                          "arrival_time_min": walkmin + node["arrival_time_min"], "distance": dst,
                          "instruction": 'walk'})

    # Amelyik vonalak megállnak ebben a megállóban azoknak az állomásai
    stop_id = node["stop_id"]
    arrival_time_min = node["arrival_time_min"]

    df_megallo_jaratok = pd.merge(
        df_stop_times.query(f'stop_id == "{stop_id}" and arrival_time_min > {arrival_time_min}'), df_trips,
        left_on="trip_id", right_on="trip_id").sort_values(["arrival_time_min"]).drop_duplicates("route_id")
    a = df_stops.query(f'stop_id == "{stop_id}"')["stop_name"]
    stop_name = a.values[0]
    df_megallo_jaratok["stop_name"] = stop_name
    df_megallo_jaratok2 = pd.merge(df_megallo_jaratok, df_routes[["route_id", "route_short_name"]], left_on="route_id",
                                   right_on="route_id")
    df_szomszedok = pd.merge(df_megallo_jaratok2, df_stop_times[
        ["trip_id", "stop_id", "arrival_time_min", "arrival_time", "departure_time", "departure_time_min"]], how="left",
                             left_on="trip_id", right_on="trip_id")
    df_szomszedok.rename(
        columns={"stop_name": "stop_name_x", "arrival_time_y": "arrival_time", "arrival_time_min_y": "arrival_time_min",
                 "stop_id_y": "stop_id", }, inplace=True)
    # Túl korai járatokat törlni
    indices_to_drop = df_szomszedok[df_szomszedok["arrival_time_min"] <= arrival_time_min].index
    df_szomszedok.drop(indices_to_drop, inplace=True)
    # Saját magamat sem akarom látni mint szomszédot
    indices_to_drop = df_szomszedok[df_szomszedok["stop_id_x"] == df_szomszedok["stop_id"]].index
    df_szomszedok.drop(indices_to_drop, inplace=True)
    df_szomszedok2 = pd.merge(df_szomszedok, df_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], how="left",
                              left_on="stop_id", right_on="stop_id")

    df_szomszedok2["instruction"] = "take"

    retdict = df_szomszedok2[
        ["stop_id", "trip_id", "route_short_name", "stop_name", "arrival_time_min", "instruction", "stop_lon",
         "stop_lat", "route_id"]].to_dict(
        'records')

    for nb in retdict:
        dst = hvdst(node["stop_id"],nb["stop_id"])
        nb["distance"]=dst

    return neigbours + retdict


# h: Heuristic function h(n) that estimates cost from n to goal
def h(node1_id, node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    dst = mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])
    return getminutestowalk(dst)

# h: Heuristic function h(n) that estimates cost from n to goal
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


def reconstruct_path(came_from, current):
    total_path = [current]
    current_node_id = current["stop_id"]
    while current_node_id in came_from:
        current = came_from[current["stop_id"]]
        current_node_id = current["stop_id"]
        total_path.insert(0, current)
    return total_path


def print_results(total_path):
    for node in total_path:
        if "instruction" not in node.keys():
            print(node)
            continue

        if node["instruction"].startswith("walk"):
            dst = node.get("distance", -1)
            istr = f'walk {dst:>5}m to  {stops[node["stop_id"]]["stop_name"]:<30} {node["stop_id"]:6} arrive at {min_to_str_time(node["arrival_time_min"])}'
        elif node["instruction"].startswith("take"):
            istr = f'take {node["route_short_name"]:<6} to  {node["stop_name"]:<30} {node["stop_id"]:6} arrive at {min_to_str_time(node["arrival_time_min"])}'
        elif node["instruction"].startswith("start"):
            node["stop_name"] = stops[node["stop_id"]]["stop_name"]
            istr = f'Start       at  {node["stop_name"]:<30} {node["stop_id"]:6} at {min_to_str_time(node["arrival_time_min"])}'

        elif node["instruction"].startswith("arrive"):
            node["stop_name"] = stops[node["stop_id"]]["stop_name"]
            istr = f'Arrive      at  {node["stop_name"]:<30} {node["stop_id"]:6} at {min_to_str_time(node["arrival_time_min"])}'
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
        get_neighbours: Function that returns neighbors of a stop_id
        d: Function d(current_node["node_id"], neighbor) that returns edge weight

    Returns:
        Path from start_node["stop_id"] to goal, or None if no path exists
    """

    goal_id = goal_node["stop_id"]
    # The set of discovered nodes (using a min-heap)
    open_set = []
    # Counter to avoid any possibility of equal values (distances) that would
    # confuse_min heap calucations
    # using a 'counter' this way will also guarantee, that the first inserted
    # value come back as first if the distabce (primary measure) is the same

    counter = 0
    # heapq.heappush(where, tuple of ( distance, actual_value))
    # heapq.heappush(open_set, (h(start_node["stop_id"]), start_node["stop_id"]))

    # heapq.heappush(where, tuple of ( distance, counter - which will never be equal to anything, actual_value))
    heapq.heappush(open_set, (start_node["arrival_time_min"] + h(start_node["stop_id"], goal_node["stop_id"]), counter,
                              start_node))
    counter = counter + 1

    # For tracking the path
    came_from = {}

    # Cost from start_node["stop_id"] to each stop_id
    g_score = defaultdict(lambda: float('inf'))
    g_score[start_node["stop_id"]] = 0

    # Estimated total cost from start_node["stop_id"] to goal through each stop_id
    f_score = defaultdict(lambda: float('inf'))
    # f_score[start_node["stop_id"]] = h(start_node["stop_id"])
    f_score[start_node["stop_id"]] = h(start_node["stop_id"], goal_node["stop_id"]) + start_node["arrival_time_min"]
    print(f"Start time: {start_node["arrival_time_min"]} estimated walking is {h(start_node["stop_id"], goal_node["stop_id"])}")
    # Track nodes in open_set for membership testing
    open_set_hash = {start_node["stop_id"]}

    while open_set:
        # Get stop_id with lowest f_score
        # heapq will guarantee this
        # minutes, counter, node

        os_min, os_cntr, current_node = heapq.heappop(open_set)
        open_set_hash.remove(current_node["stop_id"])

        #print("POP")

        if current_node["stop_id"] in ["F00189", "F00181", "F00183"] :
            print("Jo megallo")

        if current_node["stop_id"] == goal_id:
            total_path = reconstruct_path(came_from, current_node)
            print(f"Solution found! Touched {len(came_from)} stations. Length of path {len(total_path)}")
            return total_path

        neighbors = get_neighbours(current_node)
        for idx, neighbor in enumerate(neighbors):

            if neighbor["stop_id"] in ["F00189", "F00181", "F00183"]:
                print("Jo megallo")
                #tp = reconstruct_path(came_from, neighbor)
                #print_results(tp)

            # Calculate tentative g_score
            # tentative_g_score = neighbor["arrival_time_min"] + d(current_node["stop_id"], neighbor["stop_id"])
            tentative_g_score = neighbor["arrival_time_min"]

            if tentative_g_score > start_node["arrival_time_min"] + 60:
                continue

            #if neighbor["stop_id"] == "F02578" or neighbor["stop_id"] == "F02597" or neighbor["stop_id"] =='F01083' :
            #    print(neighbor)
            # try:
            #     if neighbor["route_id"] == "3060":
            #         print(neighbor)
            # except:
            #     pass

            g_score_neighbor = g_score[neighbor["stop_id"]]
            if tentative_g_score < g_score_neighbor:
                # This path is better than any previous one
                came_from[neighbor["stop_id"]] = current_node
                # g actual cost to get here
                g_score[neighbor["stop_id"]] = tentative_g_score
                # f = g + h
                f_score[neighbor["stop_id"]] = tentative_g_score + h(neighbor["stop_id"], goal_id)

                debug = 0
                if debug == 1:
                    print(f'{current_node["stop_id"]:6} {current_node["stop_name"]:30} to {neighbor["stop_id"]:6} {neighbor["stop_name"]:30} {neighbor["instruction"]:5}',end="")
                    try:
                        print(f'{neighbor["route_short_name"]:5}',end="")
                    except:
                        print('     ', end="")
                    print(f' G_curr: {g_score[current_node["stop_id"]]:8.3f} G_nb: {tentative_g_score:8.3f}    F_curr: {f_score[current_node["stop_id"]]:8.3f} {current_node["stop_id"]:6}  F_nb: {f_score[neighbor["stop_id"]]:8.3f} total_dst:{hvdst(neighbor["stop_id"],goal_id):8.2f}')

                if neighbor["stop_id"] in ["F00189", "F00181", "F00183"]:
                    print("Jo megallo")
                    #tp = reconstruct_path(came_from, neighbor)
                    #print_results(tp)

                if neighbor["stop_id"] not in open_set_hash:

                    #if neighbor["stop_id"] == "F02578" or neighbor["stop_id"] == "F02597" or neighbor["stop_id"] == 'F01083':
                    #    print(neighbor)

                    heapq.heappush(open_set, (f_score[neighbor["stop_id"]], counter, neighbor))
                    counter = counter + 1
                    open_set_hash.add(neighbor["stop_id"])
                    # heapq experienced errors if 2 values was present with the same key (distance)
                    # try:
                    #     heapq.heappush(open_set, (f_score[neighbor["stop_id"]], neighbor))
                    #     open_set_hash.add(neighbor["stop_id"])
                    # except:
                    #     print(f'Hiba:  {f_score[neighbor["stop_id"]]} {neighbor["stop_id"]} {stops[neighbor["stop_id"]]["stop_name"]:<30} ')
        #print("next POP")

    # No path found
    print("No path found")
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

arrival_time_min = str_time_to_min("09:02:00")
nF00191 = {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min}
nF00189 = {"stop_id": "F00189", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 2}
nF01081 = {"stop_id": "F01081", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 5}

rvF00191 = get_neighbours(nF00191)
rvF00189 = get_neighbours(nF00189)
rvF00181 = get_neighbours(nF01081)

arrival_time_min = str_time_to_min("09:02:00")

test_journeys = [
    {
        # "start_stop": "F00191", # Margit híd, budai hídfő H
        # "final_stop": "F01083" # Oktogon
        "start_node": {"stop_id": "F00191", "instruction": 'start', 'arrival_time_min': arrival_time_min,"stop_name": "start mhid"},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600, "stop_name": "end_oktogon"}
    },
    {
        "start_node": {"stop_id": "008152", "instruction": 'start', 'arrival_time_min': arrival_time_min},
        "goal_node": {"stop_id": "008163", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F01755", # Örs vezér tere M+H'
        # "final_stop": "F01083" #  Oktogon M
        "start_node": {"stop_id": "F01755", "instruction": 'start', 'arrival_time_min': arrival_time_min},
        "goal_node": {"stop_id": "F01083", "instruction": 'arrive', 'arrival_time_min': arrival_time_min + 600}
    },
    {
        # "start_stop": "F02268", # KFKI
        # "final_stop": "009019" # Gödöllő
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

    total_path = a_star(start_node, goal_node, h, get_neighbours, h)

    print_results(total_path)



print()
