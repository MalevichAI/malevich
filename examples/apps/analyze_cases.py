import pandas as pd
from malevich.square import processor, scheme, DFS, DF, M, Context, OBJ, Docs
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

@scheme()
class PersonalData(BaseModel):
    """Model for personal data dataframe

    Fields:
        name (str): name of the suspect
        surname (str): surname of the suspect
        occupation (str): occupation of the suspect
        date_of_birth (datetime): date of birth of the suspect
        image_url (str): URL link leading to the image of the suspect
    """
    name: str
    surname: str
    occupation: str
    date_of_birth: datetime
    image_url: str
    
@scheme()
class Cases(BaseModel):
    """Model for case metadata dataframe

    Fields:
        date (datetime): date and time of crime
        place (str): location of the crime
    """
    date: datetime
    place: str
    
@scheme()
class Suspect(BaseModel):
    """Model for suspect document

    Fields:
        name (str): name of the suspect
        surname (str): surname of the suspect
        motives (list[str]): list of possible motives to commit the crime
        cues (list[str]): list of cues
        alibi (str): alibi of the suspect
    """
    name: str
    surname: str
    motives: list[str] = None
    cues: list[str] = None
    alibi: str = None
    
@processor()
def analyze_cases(data: DFS[DF[PersonalData], M[OBJ], DF[Cases]], context: Context) -> DFS[Docs[Suspect], M[OBJ]]:
    """Processor for analyzing crime cases based on personal data of suspects, images of crime scenes and metadata of cases

    Args:
        data (DFS[DF[PersonalData], M[OBJ], DF[Cases]]): list of objects
            - DF[PersonalData]: dataframe with personal data of suspects
            - M[OBJ]: arbitrary set of images from crime scenes
            - DF[Cases]: dataframe with the metadata of crime cases
        context (Context): context. Not used in the processor

    Returns:
        DFS[Docs[Suspect], M[OBJ]]: list of objects. Consists of JSON documents with possible culprits' data (motive, cues, alibis) for each case and their images
    """
    ... # Some calculations
    return [[{
        'name': 'Aaron', 'surname': 'DeAron', 
        'motives': ['Got offended by victim\'s statements'], 
        'cues': ['Possesses a gun', 'Scarily good at basketball'],
        'alibi': 'Was spotted in a local KFC spot'
    }], None]