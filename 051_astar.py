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


def neighbours(node):
    neigbours = []

    #Ahova at lehet sétálni ebben a megállóban
    for s in stop_and_nearstops[node]["nearest_stops"]:
        neigbours.append(s["stop_id"])

    #Amelyik vonalak megállnak ebben a megállóban azoknak az állomásai

    if routes_available_from_stops.get(node) != None:
        for r in routes_available_from_stops[node]["routes"]:
            for s in stops_along_routes_nodirections[r]["list_of_stops"]:
                neigbours.append(s["stop_id"])

    return neigbours

#h: Heuristic function h(n) that estimates cost from n to goal
def h(node1_id,node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    return mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])

#d: Function d(current, neighbor) that returns edge weight
def d(node1_id,node2_id):
    node1 = stops[node1_id]
    node2 = stops[node2_id]
    return mod_distance.haversine_distance(node1["stop_lat"], node1["stop_lon"], node2["stop_lat"], node2["stop_lon"])


def reconstruct_path(came_from, current):
    total_path = [current]
    while current in came_from:
        current = came_from[current]
        total_path.insert(0, current)
    return total_path


def a_star(start, goal, h, neighbors, d):
    """
    A* pathfinding algorithm.

    Args:
        start: Starting node
        goal: Goal node
        h: Heuristic function h(n) that estimates cost from n to goal
        neighbors: Function that returns neighbors of a node
        d: Function d(current, neighbor) that returns edge weight

    Returns:
        Path from start to goal, or None if no path exists
    """
    # The set of discovered nodes (using a min-heap)
    open_set = []
    #heapq.heappush(open_set, (h(start), start))
    heapq.heappush(open_set, (h(start,goal), start))


    # For tracking the path
    came_from = {}

    # Cost from start to each node
    g_score = defaultdict(lambda: float('inf'))
    g_score[start] = 0

    # Estimated total cost from start to goal through each node
    f_score = defaultdict(lambda: float('inf'))
    #f_score[start] = h(start)
    f_score[start] = h(start,goal)


    # Track nodes in open_set for membership testing
    open_set_hash = {start}

    while open_set:
        # Get node with lowest f_score
        _, current = heapq.heappop(open_set)
        open_set_hash.remove(current)

        if current == goal:
            return reconstruct_path(came_from, current)

        for neighbor in neighbors(current):
            # Calculate tentative g_score
            tentative_g_score = g_score[current] + d(current, neighbor)

            if tentative_g_score < g_score[neighbor]:
                # This path is better than any previous one
                came_from[neighbor] = current
                g_score[neighbor] = tentative_g_score
                f_score[neighbor] = tentative_g_score + h(neighbor,goal)

                if neighbor not in open_set_hash:
                    heapq.heappush(open_set, (f_score[neighbor], neighbor))
                    open_set_hash.add(neighbor)

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


neighbours("008152")

total_path = a_star("008152","008163", h, neighbours, h)

print(total_path)
for s in total_path:
    print(stops[s]["stop_name"])