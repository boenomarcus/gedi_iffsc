import geojson

from shapely.geometry import Point, Polygon
from utils import strings, numbers, config


def shapelyPol_from_GeoJSONSinglePol(geo_filepath):
    
    # Open GeoJSON file
    with open(geo_filepath) as f:
        gj = geojson.load(f)

    # Subset info of interest
    coords = gj["features"][0]["geometry"]["coordinates"][0]

    # Return shapely polygon
    return Polygon([tuple([pair[1], pair[0]]) for pair in coords])