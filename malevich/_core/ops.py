"""
This module contains safe and fast version of
batched operations on Malevich Core

"""
import os
from concurrent.futures import Future, ProcessPoolExecutor
from multiprocessing import cpu_count
from typing import Optional

import malevich_coretools as core
from malevich_coretools import FilesDirs

from malevich.constants import DEFAULT_CORE_HOST
from malevich.models import AssetNode, Collection

executor = ProcessPoolExecutor(max_workers=cpu_count())


def result_collection_name(operation_id: str, alias: str = '') -> str:
    return f"result-{operation_id}-{alias}"


def _create_app_safe(
        app_id: str,
        extra: dict,
        uid: str,
        auth: core.AUTH = None,
        conn_url: Optional[str] = DEFAULT_CORE_HOST,
        *args,
        alias: str = '',
        **kwargs,
) -> None:
    settings = core.AppSettings(
        appId=app_id,
        taskId=app_id,
        saveCollectionsName=result_collection_name(uid, alias)
    )

    kwargs_ = {
        "app_id": app_id,
        "app_cfg": kwargs['app_cfg'],
        "image_ref": kwargs['image_ref'],
        "extra_collections_from": kwargs['extra_collections_from'],
    }

    try:
        core.create_app(
            app_id,
            processor_id=extra["processor_id"],
            auth=auth,
            conn_url=conn_url,
            *args,
            **kwargs
        )
    except Exception:
        pass
    else:
        return settings, kwargs_

    try:
        if core.get_app(
            app_id,
            auth=auth,
            conn_url=conn_url,
        ):
            core.delete_app(
                app_id,
                auth=auth,
                conn_url=conn_url,
            )
            core.delete_task(
                app_id,
                auth=auth,
                conn_url=conn_url,
            )
    except Exception:
        pass

    try:
        core.create_app(
            app_id,
            processor_id=extra["processor_id"],
            auth=auth,
            conn_url=conn_url,
            *args,
            **kwargs
        )
    except Exception as e:
        if 'processor_id' in extra:
            processor_id = extra['processor_id']
            raise Exception(
                f"Failed to create an app. Processor is {processor_id}. "
            ) from e
        else:
            raise Exception(
                "Failed to create an app and could determine the processor. "
                "Most probably, the app is installed incorrectly. Use "
                "malevich remove to remove it and reinstall it correctly"
            ) from e

    return settings, kwargs


def batch_create_apps(
    kwargs_list: list[dict],
) -> list[tuple[core.AppSettings, dict]]:
    results: list[Future] = []
    for kwargs_ in kwargs_list:
        results.append(executor.submit(_create_app_safe, **kwargs_))

    return [r.result() for r in results]


def _create_task_safe(
    auth: core.AUTH = None,
    conn_url: Optional[str] = DEFAULT_CORE_HOST,
    **task_kwargs,
) -> None:
    core.create_task(
        **task_kwargs,
        auth=auth,
        conn_url=conn_url,
    )


def batch_create_tasks(
    kwargs_list: list[dict],
    auth: core.AUTH = None,
    conn_url: Optional[str] = DEFAULT_CORE_HOST,
) -> None:
    # results: list[Future] = []
    # for kwargs_ in kwargs_list:
    #     results.append(executor.submit(_create_task_safe, **kwargs_))

    # return [r.result() for r in results]
    with core.Batcher(auth=auth, conn_url=conn_url):
        for kwargs_ in kwargs_list:
           _create_task_safe(**kwargs_)


def _upload_collection(
    collection: Collection,
    auth: core.AUTH = None,
    conn_url: Optional[str] = None,
) -> str:
    if collection.collection_data is None:
        raise Exception(
            f"Trying to upload collection {collection.collection_id} "
            "without data. Probably you set persistent=True and core_id, "
            "but collection was not found in Core."
        )

    if collection.collection_data is not None:
        collection.core_id = core.create_collection_from_df(
            data=collection.collection_data,
            name=collection.magic(),
            auth=auth,
            conn_url=conn_url,
        )

    return collection.core_id


def _assure_collection(
    collection: Collection,
    auth: core.AUTH = None,
    conn_url: Optional[str] = None,
) -> str:
    if collection.core_id:
        try:
            core.get_collection(collection.core_id, auth=auth, conn_url=conn_url)
            return collection.core_id
        except Exception as e:
            raise Exception(
                f"Collection {collection.collection_id} with core_id "
                f"{collection.core_id} is not found in Core. Do not "
                "specify core_id if you want to upload the collection."
            ) from e

    _ids = core.get_collections_by_name(
        collection.magic(),
        conn_url=conn_url,
        auth=auth,
    )

    if len(_ids.ownIds) == 0:
        _upload_collection(collection, auth, conn_url)
    else:
        collection.core_id = _ids.ownIds[0]

    # if collection.core_id not in core.get_collections(
    #     conn_url=conn_url,
    #     auth=auth
    # ).ownIds:
    #     raise Exception(
    #         f"Collection {collection.collection_id} with core_id "
    #         f"{collection.core_id} is not found in Core."
    #     )

    return collection.core_id


def batch_upload_collections(
    collections: list[Collection],
    auth: core.AUTH = None,
    conn_url: Optional[str] = None,
) -> list[str]:
    results: list[Future] = []
    for collection in collections:
        results.append(executor.submit(_assure_collection, collection, auth, conn_url))

    results = [r.result() for r in results]

    assert len(set(results)) == len(results), \
        "Some collections have the same core_id"

    return results

def _join_path(_path: str, _name: str) -> str:
    if _path.endswith("/"):
        return _path + _name
    else:
        return _path + "/" + _name

def _upload_asset(
    asset: AssetNode,
    auth: core.AUTH = None,
    conn_url: Optional[str] = None,
) -> None:
    if asset.is_composite:
        for f_ in asset.real_path:
            core.update_collection_object(
                _join_path(asset.core_path, os.path.basename(f_)),
                data=open(f_, "rb").read(),
                auth=auth,
                conn_url=conn_url,
            )
    else:
        core.update_collection_object(
            asset.core_path,
            data=open(asset.real_path, "rb").read(),
            auth=auth,
            conn_url=conn_url,
        )

def _assure_asset(
    asset: AssetNode,
    auth: core.AUTH = None,
    conn_url: Optional[str] = None,
) -> None:
    try:
        if not asset.is_composite:
            core.get_collection_object(
                asset.core_path,
                auth=auth,
                conn_url=conn_url,
            )
            objs = FilesDirs(files={asset.core_path: 0}, directories=[])
        else:
            objs = core.get_collection_objects(
                asset.core_path,
                auth=auth,
                conn_url=conn_url,
            )
    except Exception as e:
        if asset.real_path is not None:
            _upload_asset(asset, auth, conn_url)
            return
        else:
            raise Exception(
                f"Asset {asset.core_path} is not found in Core. Cannot "
                "upload it because real_path is not specified."
            ) from e

    if asset.real_path is not None:
        if asset.is_composite:
            for f_ in asset.real_path:
                if os.path.basename(f_) not in objs.files:
                    _upload_asset(asset, auth, conn_url)
                    break
        else:
            if asset.core_path not in objs.files:
                _upload_asset(asset, auth, conn_url)
