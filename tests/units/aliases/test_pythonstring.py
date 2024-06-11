from malevich import asset, collection, flow
from malevich.square import Context
from pydantic import ValidationError
import pytest


def test_asset():
    with pytest.raises(ValidationError):
        file = asset.file(name='invalid asset.', path='./dummy.txt')

# def test_collection():
#     with pytest.raises(ValidationError):
#         coll = collection('inval.id coll$ct^$n')
    
# def test_flow():
#     with pytest.raises(ValidationError):
#         @flow(reverse_id="inv$^#[]d rev--id~")
#         def invalid_function(df, ctx: Context):
#             return None
        