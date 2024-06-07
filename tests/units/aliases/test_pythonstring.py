from malevich import asset, collection, flow
from malevich.square import Context
import pytest


def test_asset():
    with pytest.raises(Exception):
        file = asset.file(name='invalid asset.')

def test_collection():
    with pytest.raises(Exception):
        coll = collection('inval.id coll$ct^$n')
    
def test_flow():
    with pytest.raises(Exception):
        @flow(reverse_id="inv$^#[]d rev--id~")
        def invalid_function(ctx: Context):
            return None
        
        invalid_function()