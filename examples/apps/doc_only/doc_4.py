import pandas as pd
from malevich.square import processor, scheme, Context, Doc
from pydantic import BaseModel, Field
from typing import Optional

@scheme()
class Input(BaseModel):
    """Model for input data of the search task

    Fields:
        array (list[int]): sorted array of numbers
        prompts (list[int]): numbers to find
    """
    array: list[int]
    prompts: list[int]
    
    
@scheme()
class ClosestEntries(BaseModel):
    """Model for output data of the search task

    Fields:
        result (list[tuple[int, int]]): list of closest numbers to prompted ones and their indices    
    """
    result: list[tuple[int, int]]
    
@processor()
def binary_search(data: Doc[Input], context: Context) -> Doc[ClosestEntries]:
    """Processor for efficient less or equal number lookup in the sorted array. Works in O(M log N) time complexity, where M - number of queries and N - length of the array.
    Utilizes a binary search for each prompted number

    Args:
        data (Doc[Input]): given sorted array (non-descending order) and a list of numbers to look up
        context (Context): context. Not used in the processor
    Returns:
        Doc[ClosestEntries]: list of closest numbers in the array and their indices. Has to be the same length as a prompt array
    """
    ... # Some calculations
    return {
        'result': [(3, 0), (4, 1), (5, 5)] 
    }