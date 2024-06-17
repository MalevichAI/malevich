from malevich import Core, flow, table, collection
from malevich.utility import rename

@flow
def test_extension():
    col1 = collection(
        'test_extension_col',
        df=table(
            {'data': ['123', '321']}
        )
    )

    rename('')