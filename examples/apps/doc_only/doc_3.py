import pandas as pd
from malevich.square import processor, scheme, Context, Doc
from pydantic import BaseModel, Field
from typing import Optional

@scheme()
class GeoData(BaseModel):
    """Model for geolocation metadata

    Fields:
        start_lat (float): starting point's latitude
        start_lon (float): starting point's longitude
        end_lat (float): endpoint's latitude
        end_lon (float): endpoint's longitude
    """
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    
@scheme()
class Distance(BaseModel):
    """Model for case metadata dataframe

    Fields:
        date (datetime): date and time of crime
        place (str): location of the crime
    """
    dist: float
    
@processor()
def calculate_distance(data: Doc[GeoData], context: Context) -> Doc[Distance]:
    """Processor for calculating the distance between two points given their coordinates in geographic coordinate system (GCS).

    Args:
        data (Doc[GeoData]): start and end point's coordinates in GCS
        context (Context): context. Not used in the processor

    Returns:
        Doc[Distance]: the distance between the points in kilometers
    """
    ... # Some calculations
    return {'dist': 10.5667}