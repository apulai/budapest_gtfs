import math

def equirectangular_distance(lat1,lon1,lat2,lon2):
    rad_lat1 = lat1 * math.pi / 180.0
    rad_lon1 = lon1 * math.pi / 180.0
    rad_lat2 = lat2 * math.pi / 180.0
    rad_lon2 = lon2 * math.pi / 180.0

    x = (rad_lon2-rad_lon1)* math.cos((rad_lat1+rad_lat2)/2)
    y = (rad_lat2 - rad_lat1)
    dst = math.sqrt( x*x + y*y) * 111.139 * 60 * 1000
    return dst

def haversine_distance(lat1,lon1,lat2,lon2):

    R = 6371
    rad_lat1 = lat1 * math.pi / 180.0
    rad_lon1 = lon1 * math.pi / 180.0
    rad_lat2 = lat2 * math.pi / 180.0
    rad_lon2 = lon2 * math.pi / 180.0
    dlat = (lat2 - lat1) * math.pi / 180.0
    dlon = (lon2 - lon1) * math.pi / 180.0

    a = math.sin(dlat/2) * math.sin( dlat / 2) + \
        math.cos(rad_lat1) * math.cos(rad_lat2) * \
        math.sin(dlon/2)*math.sin(dlon/2)

    c = 2* math.atan(math.sqrt(math.fabs(a)))
    dst = R * c * 1000

    return dst