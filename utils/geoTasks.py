"""
GEO Tasks utilities

Functions to perform geospatial operations

Author: Marcus Moresco Boeno

"""

# Third party library imports
import geojson
from shapely.geometry import Point, Polygon

# Local application imports
from utils import strings, numbers, config


def shapelyPol_from_GeoJSONSinglePol(geo_filepath):
    """
    > shapelyPol_from_GeoJSONSinglePol(geo_filepath)
        Read GeoJSON Single polygon and convert to a Shapely Polygon.

    > Arguments:
        - geo_filepath: Full path to GeoJSON file.
    
    > Output:
        - shapely.geometry.Polygon feature.
    """
    # Return shapely polygon
    return Polygon([tuple([pair[1], pair[0]]) for pair in coords])

