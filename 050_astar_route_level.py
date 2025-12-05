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


def get_neighbours(stop_id):
    neigbours = []
    routing = []

    #Ahova at lehet sétálni ebben a megállóban
    for s in stop_and_nearstops[stop_id]["nearest_stops"]:
        dst = int(s["distance"])
        neigbours.append( { "stop_id": s["stop_id"], "distance": dst, "instruction": 'walk' } )


    #Amelyik vonalak megállnak ebben a megállóban azoknak az állomásai
    if routes_available_from_stops.get(stop_id) != None:
        for r in routes_available_from_stops[stop_id]["routes"]:
            # Kizártam az éjszakai járatokat, mert azokat nem ismerem
            # Meg valamiért előszerettel választja az algoritmus
            if r.isnumeric() and int(r) > 8999 :
                continue
            for s in stops_along_routes_nodirections[r]["list_of_stops"]:
                neigbours.append({ "stop_id": s["stop_id"], "route": r, "instruction": "take" })

    return neigbours

#h: Heuristic function h(n) that estimates cost from n to goal
def h(node1_id,node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    return mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])

#d: Function d(current_node["node_id"], neighbor) that returns edge weight
def d(node1_id,node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    return mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])


def reconstruct_path(came_from,  current):
    total_path = [current]
    current_node_id = current["stop_id"]
    while current_node_id in came_from:
        current = came_from[current["stop_id"]]
        current_node_id = current["stop_id"]
        total_path.insert(0, current)
    return total_path


def a_star(start_id, goal_id, h, get_neighbours, d):
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
    # The set of discovered nodes (using a min-heap)
    open_set = []
    # Counter to avoid any possibility of equal values (distances) that would
    # confuse_min heap calucations
    # using a 'counter' this way will also guarantee, that the first inserted
    # value come back as first if the distabce (primary measure) is the same

    counter = 0
    #heapq.heappush(where, tuple of ( distance, actual_value))
    #heapq.heappush(open_set, (h(start_node["stop_id"]), start_node["stop_id"]))
    start_node = { "stop_id": start_id, "instruction": 'start' }
    goal_node = { "stop_id": goal_id, "instruction": 'arrive' }
    #heapq.heappush(where, tuple of ( distance, counter - which will never be equal to anything, actual_value))
    heapq.heappush(open_set, (h(start_node["stop_id"],goal_node["stop_id"]), counter, start_node))
    counter = counter + 1


    # For tracking the path
    came_from = {}

    # Cost from start_node["stop_id"] to each stop_id
    g_score = defaultdict(lambda: float('inf'))
    g_score[start_node["stop_id"]] = 0

    # Estimated total cost from start_node["stop_id"] to goal through each stop_id
    f_score = defaultdict(lambda: float('inf'))
    #f_score[start_node["stop_id"]] = h(start_node["stop_id"])
    f_score[start_node["stop_id"]] = h(start_node["stop_id"],goal_node["stop_id"])


    # Track nodes in open_set for membership testing
    open_set_hash = {start_node["stop_id"]}

    while open_set:
        # Get stop_id with lowest f_score
        # heapq will guarantee this
        # distance, counter, node
        _, _, current_node = heapq.heappop(open_set)
        open_set_hash.remove(current_node["stop_id"])


        if current_node["stop_id"] == goal_id:
            total_path = reconstruct_path(came_from, current_node)
            print(f"Solution found! Touched {len(came_from)} stations. Length of path {len(total_path)}")
            return total_path

        neighbors  = get_neighbours(current_node["stop_id"])
        for idx,neighbor in enumerate(neighbors):
            # Calculate tentative g_score
            tentative_g_score = g_score[current_node["stop_id"]] + d(current_node["stop_id"], neighbor["stop_id"])

            if tentative_g_score < g_score[neighbor["stop_id"]]:
                # This path is better than any previous one
                came_from[neighbor["stop_id"]] = current_node

                g_score[neighbor["stop_id"]] = tentative_g_score
                f_score[neighbor["stop_id"]] = tentative_g_score + h(neighbor["stop_id"],goal_id)

                if neighbor["stop_id"] not in open_set_hash:
                    heapq.heappush(open_set, (f_score[neighbor["stop_id"]], counter, neighbor))
                    counter = counter + 1
                    open_set_hash.add(neighbor["stop_id"])
                    # heapq experienced errors if 2 values was present with the same key (distance)
                    # try:
                    #     heapq.heappush(open_set, (f_score[neighbor["stop_id"]], neighbor))
                    #     open_set_hash.add(neighbor["stop_id"])
                    # except:
                    #     print(f'Hiba:  {f_score[neighbor["stop_id"]]} {neighbor["stop_id"]} {stops[neighbor["stop_id"]]["stop_name"]:<30} ')

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
print()
print()


get_neighbours("008152")


# start_stop="008152" # Mester utca / könyveskalman
# final_stop="008163" # Újpest Városkapu

#start_stop="F00191" # Margit híd, budai hídfő H
#final_stop="F01083" # Király utca / Erzsébet körút
#
# start_stop="F01755" # Margit híd, budai hídfő H
# final_stop="008163" # Újpest Városkapu
#
# start_stop="F02268" # KFKI
# final_stop="009019" # Gödöllő

test_journeys = [{
    "start_stop":"008152", # Mester utca / könyveskalman
    "final_stop":"008163" # Újpest Városkapu
    },
    {
    "start_stop": "F00191", # Margit híd, budai hídfő H
    "final_stop": "F01083" # Király utca / Erzsébet körút
    },
    {
    "start_stop": "F01755", # Örs vezér tere M+H'
    "final_stop": "F01083" #  Oktogon M
    },
    {
    "start_stop": "F02268", # KFKI
    "final_stop": "009019" # Gödöllő
    }
]

for journey in test_journeys:
    print()
    print("---")

    total_path = a_star(journey["start_stop"], journey["final_stop"], h, get_neighbours, h)

    print(f'{stops[journey["start_stop"]]["stop_name"]} --> {stops[journey["final_stop"]]["stop_name"]}')
    for node in total_path:
        if node["instruction"].startswith("walk"):
            dst = node.get("distance",-1)
            istr = f'walk {dst:>5}m to  {stops[node["stop_id"]]["stop_name"]} {node["stop_id"]}'
        elif node["instruction"].startswith("take"):
            istr = f'take {routes[node["route"]]["route_short_name"]:<6} to  {stops[node["stop_id"]]["stop_name"]:<30} headsign {routes[node["route"]]["route_desc"]:<70}  {node["stop_id"]}'
        elif node["instruction"].startswith("start"):
            istr = f'Start       at  {stops[node["stop_id"]]["stop_name"]} {node["stop_id"]}'
        elif node["instruction"].startswith("arrive"):
            istr = f'Arrive      at  {stops[node["stop_id"]]["stop_name"]} {node["stop_id"]}'
        else:
            istr = f'Should not reach this!'

        print(istr)


    print()