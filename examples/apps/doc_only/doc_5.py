import pandas as pd
from malevich.square import processor, scheme, Context, Doc
from pydantic import BaseModel, Field
from typing import Optional

@scheme()
class Genre(BaseModel):
    """Model for desired genre

    Fields:
        genre (str): genre of games to find
    """
    genre: str
    
    
@scheme()
class Games(BaseModel):
    """Model for recommended games' metadata 

    Fields:
        names (list[str]): list of names
        tags (dict[str, list[str]]): tags for each game
        rating (dict[str, float]): rating for each game
        year (dict[str, int]): years in which games were created
    """
    names: list[str]
    tags: dict[str, list[str]]
    rating: dict[str, float]
    year: dict[str, int]
    
    
@processor()
def binary_search(data: Doc[Genre], context: Context) -> Doc[Games]:
    """Processor for recommending games of prompted genre. Obtains the data from SteamDB

    Args:
        data (Doc[Genre]): required genre
        context (Context): context. Not used in the processor
    Returns:
        Doc[Games]: list of game titles, their ratings, tags and year in which they were created
    """
    ... # Some calculations
    return {
        'names': ['Resident Evil', 'Silent Hill', 'Dead Space'],
        'tags': {
            'Resident Evil': ['Gore', 'Puzzle', 'Fixed Camera'],
            'Silent Hill': ['Gore','Puzzle'],
            'Dead Space': ['Gore', 'Action', 'Survival']
        },
        'rating': {
            'Resident Evil': 4.78,
            'Silent Hill': 4.68,
            'Dead Space': 4.55
        },
        'year': {
            'Resident Evil': 1996,
            'Silent Hill': 1999,
            'Dead Space': 2008 
        }
    }