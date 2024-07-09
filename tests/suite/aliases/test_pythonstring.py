from malevich import asset, collection, flow, table

import pytest


def test_asset():
    with pytest.raises(ValueError):
        @flow()
        def check_asset():
            file = asset.from_file(name='invalid. asset', path='tests/suite/assets/file.txt')

        check_asset()

def test_collection():
    with pytest.raises(ValueError):
        @flow()
        def check_collection():
            coll = collection(name='inval.id coll$ct^$n', df=table({'data': [1, 2, 3]}))

        check_collection()
    
# def test_flow():
#     with pytest.raises(ValueError):
#         @flow(reverse_id="inv$^#[]d rev--id~")
#         def invalid_function(df, ctx: Context):
#             return None
        