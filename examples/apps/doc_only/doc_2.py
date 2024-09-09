import pandas as pd
from malevich.square import processor, scheme, Context, Doc
from pydantic import BaseModel, Field
from typing import Optional
from haversine import haversine

@scheme()
class GeoPoint(BaseModel):
    """Model for geolocation metadata

    Fields:
        lat (float): latitude
        lon (float): longitude
        radius (float): radius around the location for lookup
        places (dict[str, tuple[float, float]]): places in the area and their coordinates
    """
    lat: float
    lon: float
    radius: float
    places: dict[str, tuple[float, float]]
    
@scheme()
class Places(BaseModel):
    """Model for nearby places metadata

    Fields:
        names (list[str]): names of the places
        coords (list[tuple[tuple[float, float], float]]): list of coordinates (latitude and longitude) and the distance from the starting point 
    """
    names: list[str]
    coords: list[tuple[tuple[float, float], float]]
    
@processor()
def recommend_places(data: Doc[GeoPoint], context: Context) -> Doc[Places]:
    """Processor for finding places of interest nearby a specified location

    Args:
        data (Doc[GeoData]): latitude and longitude of a location in geographic coordinate system, radius around the location to look for places, places' coordinates and names
        context (Context): context. Not used in the processor
    Returns:
        Doc[Places]: names of the places nearby, their coordinates and the distance between the place and given location in kilometers
    """
    ... # Some calculations
    return {
        'names': ['Jolibee'],
        'coords': [((55.6070, 65.0031), 10.06)]
    }