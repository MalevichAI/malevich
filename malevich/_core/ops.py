"""
This module contains safe and fast version of
batched operations on Malevich Core

"""
from concurrent.futures import Future, ProcessPoolExecutor
from multiprocessing import cpu_count

import malevich_coretools as core

from ..models.collection import Collection

executor = ProcessPoolExecutor(max_workers=cpu_count())



def result_collection_name(operation_id: str) -> str:
    return f"result-{operation_id}"

def _create_app_safe(
        app_id: str,
        extra: dict,
        uid: str,
        *args,
        **kwargs,
) -> None:
        settings = core.AppSettings(
            appId=app_id,
            taskId=app_id,
            saveCollectionsName=result_collection_name(uid)
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
                *args,
                **kwargs
            )
        except Exception:
            pass
        else:
            return settings, kwargs_

        try:
            if core.get_app(app_id):
                core.delete_app(app_id)
                core.delete_task(app_id)
        except Exception:
            pass

        try:
            core.create_app(
                app_id,
                processor_id=extra["processor_id"],
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
       **task_kwargs,
) -> None:
    core.create_task(**task_kwargs)


def batch_create_tasks(
    kwargs_list: list[dict],
) -> None:
    results: list[Future] = []
    for kwargs_ in kwargs_list:
        results.append(executor.submit(_create_task_safe, **kwargs_))

    return [r.result() for r in results]

def _upload_collection(collection: Collection) -> str:
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
        )

    return collection.core_id

def _assert_collection(collection: Collection) -> str:
    if collection.core_id:
        try:
            core.get_collection(collection.core_id)
            return collection.core_id
        except Exception as e:
            raise Exception(
                f"Collection {collection.collection_id} with core_id "
                f"{collection.core_id} is not found in Core. Do not "
                "specify core_id if you want to upload the collection."
            ) from e

    _ids = core.get_collections_by_name(
        collection.magic()
    )

    if len(_ids.ownIds) == 0:
        _upload_collection(collection)
    else:
        collection.core_id = _ids.ownIds[0]

    if collection.core_id not in core.get_collections().ownIds:
        raise Exception(
            f"Collection {collection.collection_id} with core_id "
            f"{collection.core_id} is not found in Core."
        )

    return collection.core_id

def batch_upload_collections(
    collections: list[Collection],
) -> None:
    results: list[Future] = []
    for collection in collections:
        results.append(executor.submit(_assert_collection, collection))

    results = [r.result() for r in results]

    assert len(set(results)) == len(results), \
        "Some collections have the same core_id"

    return results
