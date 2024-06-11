from malevich import asset, collection, flow
from malevich.square import Context
from malevich._autoflow.no_trace import notrace

import pytest


def test_asset():
    with pytest.raises(ValueError):
        with notrace():
            file = asset.from_file(name='invalid. asset', path='tests/units/assets/file.txt')


# def test_collection():
#     with pytest.raises(ValidationError):
#         coll = collection('inval.id coll$ct^$n')
    
# def test_flow():
#     with pytest.raises(ValidationError):
#         @flow(reverse_id="inv$^#[]d rev--id~")
#         def invalid_function(df, ctx: Context):
#             return None
        