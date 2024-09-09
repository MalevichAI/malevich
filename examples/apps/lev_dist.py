import pandas as pd
from malevich.square import processor, scheme, DFS, DF, M, Context
from pydantic import BaseModel, Field
from typing import Optional

@scheme()
class LevDist(BaseModel):
    """Context model for `levenshtein_distance` processor.

    Fields:
        calculate_all_costs (bool): Determines if the processor should perform a cross-calculation, i.e. calculate the distance between each pair of strings for every provided cost set. Defaults to `False`
    """
    calculate_all_costs: Optional[bool] = Field(
        False, 
        description="Whether to calculate the distance between each pair of strings for all costs" # noqa:E501
    )

@scheme()
class Strings(BaseModel):
    """Model for a dataframe with strings

    Fields:
        from_str (str): initial string
        to_str (str): desired string 
    """
    from_str: str
    to_str: str
    
@scheme()
class Costs(BaseModel):
    """Model for a dataframe with costs

    Fields:
        insert (int): cost of insert operation
        delete (int): cost of delete operation
        replace (int): cost of replace operation
    """
    insert: int
    delete: int
    replace: int

@scheme()
class Distance(BaseModel):
    """Model for the output dataframe with calculated distances

    Args:
        dist (int): total cost of operations required
    """
    dist: int
    
    
def calculate_distance(from_str: str, to_str: str, costs: tuple[int, int, int]) -> int:
    """Auxiliary method for calculating the Levenshtein distance between two strings with a set of operation costs

    Args:
        from_str (str): initial string
        to_str (str): desired string
        costs (tuple[int, int, int]): tuple of operation costs. Insertion cost is at position 0, deletion cost - at position 1, replacement cost - at position 2.

    Returns:
        int: total cost of operations required
    """
    M, N = len(from_str), len(to_str)
    prev_row = [i for i in range(N + 1)]
    cur_row = [0 for _ in range(N + 1)]
    for i in range(1, M + 1):
        cur_row[0] = i
        for j in range(1, N + 1):
            if from_str[i - 1] == to_str[j - 1]:
                cur_row[j] = prev_row[j - 1]
            else:
                cur_row[j] = min(
                    cur_row[j - 1] + costs[0], 
                    prev_row[j] + costs[1], 
                    prev_row[j - 1] + costs[2]
                )
        prev_row = cur_row
    return cur_row[N]

@processor()
def levenshtein_distance(data: DFS[DF[Strings], DF[Costs]], context: Context[LevDist]) -> DFS[M[DF[Distance]]]:
    """Processor for calculating the Levenstein distance for cost operations. Utilizes iterative memorization backtracking approach to achieve O(M * N) time complexity, where M is the length of initial string and N is the length of desired string.

    Args:
        data (DFS[DF[Strings], DF[Costs]]): tuple of two dataframes which contain string pairs and costs of operations.
        context (Context[LevDist]): configuration

    Returns:
        DFS[M[DF[Distance]]]: one or multiple dataframes that contain the minimal cost of operations for each string pair. If `calculate_all_costs` is True, produces C dataframes, where C - number of cost sets in the input
    """
    calc_all = context.app_cfg.calculate_all_costs
    # If the number of string pairs matches the number of cost sets, simply calculate the distance for each pair
    if len(data[0]) == len(data[1]) and not calc_all:
        distances = []
        for id in data[0].index:
            costs = data[1].iloc[id, :].to_list()
            from_str, to_str = data[0].iloc[id, :].to_list()
            distances.append(calculate_distance(from_str, to_str, costs))
        return (pd.DataFrame(data={
            'dist': distances
        }))
    # Else calculate the distance for each cost
    else:
        # Assert that `calculate_all_costs` is True, so that dimensions match
        assert calc_all == True, "Number of string pairs does not match with number of cost sets. Please use `calculate_all_costs=True` for cross-calculation." # noqa:E501
        result = []
        for cost_id in data[1].index:
            costs = data[1].iloc[cost_id, :].to_list()
            distances = []
            for str_id in data[0].index:
                from_str, to_str = data[0].iloc[str_id, :].to_list()
                distances.append(calculate_distance(from_str, to_str, costs))
            result.append(pd.DataFrame(data={
                'dist': distances
            }))
        return tuple(result)