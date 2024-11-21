from malevich.core_api import FilesDirs, get_collection_objects


def get_asset_full_tree(
    base_path: str,
    conn_url: str,
    auth: tuple[str, str]
) -> FilesDirs:
    file_dirs = get_collection_objects(
        path=base_path,
        recursive=True,
        conn_url=conn_url,
        auth=auth,
    )

    return file_dirs
