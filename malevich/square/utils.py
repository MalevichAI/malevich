
import json
import logging
import pickle
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar, Union

import boto3
import jsonpickle
import numpy as np
import pandas as pd
from botocore.response import StreamingBody

from .df import OBJ

WORKDIR = "/malevich"
"""
Working directory from which the app is run.
Equivalent to :code:`os.getcwd()` from within the app.
"""

APP_DIR = f"{WORKDIR}/apps"
"""
Directory into which the user code is copied during app construction.
"""

MinimalCfg = TypeVar('MinimalCfg')

class Context(Generic[MinimalCfg]):
    """
    Context contains all the necessary information about the run
    and the environment in which it is executed. Also, context provides
    a number of auxiliary functions  for interacting with the environment, such as
    working with shared storage (:meth:`share`, :meth:`get_share_path`, :meth:`delete_share`),
    dealing with common objects (:attr:`common`),
    access to the key-value storage (:attr:`dag_key_value`),
    and object storage (:attr:`object_storage`).
    """

    class _DagKeyValue:
        """
        Simple key-value storage, shared for all apps of one run.
        Values must be bytes, string, int or float; dictionary order is not guaranteed
        """

        def __init__(self, run_id: Optional[str] = None) -> None:
            pass

        def get_bytes(self, key: str) -> bytes:
            """Gets a binary value by key more optimally.

            Consider using this to retrieve binary data in
            more efficient way, but keep in mind that :meth:`get`
            can also be used

            Args:
                key (str): key in storage

            Returns:
                bytes: Value stored by key
            """
            pass

        async def async_get_bytes(self, key: str) -> bytes:
            """Gets a binary value by key more optimally.

            Consider using this to retrieve binary data in
            more efficient way, but keep in mind that :meth:`get`
            can also be used

            Args:
                key (str): key in storage

            Returns:
                bytes: Value stored by key
            """
            pass

        def get(self, keys: List[str]) -> Dict[str, Any]:
            """Gets values by keys

            Retrieves a slice of storage by keys. If a key is not found,
            the value will be set to None

            Args:
                keys (list[str]): list of keys

            Returns:
                Dict[str, Any]: A dictionary of key-value pairs.
            """
            pass

        async def async_get(self, keys: List[str]) -> Dict[str, Any]:
            """Gets values by keys

            Retrieves a slice of storage by keys. If a key is not found,
            the value will be set to None

            Args:
                keys (list[str]): list of keys

            Returns:
                Dict[str, Any]: A dictionary of key-value pairs.
            """
            pass

        def get_all(self) -> Dict[str, Any]:
            """Gets all values

            Retrieves the whole storage as a dictionary.

            Returns:
                Dict[str, Any]: A dictionary of key-value pairs.
            """
            pass

        async def async_get_all(self) -> Dict[str, Any]:
            """Gets all values

            Retrieves the whole storage as a dictionary.

            Returns:
                Dict[str, Any]: A dictionary of key-value pairs.
            """
            pass

        def update(self, keys_values: Dict[str, Any]) -> None:
            """Sets values by keys

            Accepts a dictionary of key-value pairs and sets them in storage.
            If a key already exists, it will be overwritten.

            Args:
                keys_values (dict): A dictionary of key-value pairs.
            """
            pass

        async def async_update(self, keys_values: Dict[str, Any]) -> None:
            """Sets values by keys

            Accepts a dictionary of key-value pairs and sets them in storage.
            If a key already exists, it will be overwritten.

            Args:
                keys_values (dict): A dictionary of key-value pairs.
            """
            pass

        def clear(self) -> None:
            """Purges the storage"""
            pass

        async def async_clear(self) -> None:
            """Purges the storage"""
            pass

    class _ObjectStorage:
        """
        A storage for binary objects common to the user.

        Works with cloud S3 storage and app shared file system. Provides
        an access to the objects shared with :meth:`Context.share` method.
        and accessible with :meth:`Context.get_share_path` method.

        All methods of the object storage have :code:`local` parameter. If it is
        set to :code:`True`, the operation will be performed on the local file system.
        Otherwise, the operation will be performed on the remote object storage.

        The information might be unsynchronized between apps. To ensure that the
        information is synchronized, use :code:`all_apps` parameter. If it is set to
        :code:`True`, the operation will be performed on all apps. Otherwise, the
        operation will be performed only on apps the method is called from.

        """

        def get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:
            """
            Get keys from local mount or remote object storage.

            Args:
                local (bool, optional): whether to use local mount or remote object storage.
                    If set to :code:`True`, the operation will
                    be performed on the local file system. Otherwise,
                    the operation will be performed on the remote object storage. Defaults to False.
                all_apps (bool, optional): whether to synchronize the operation between all apps.
                    If set to :code:`True`, the operation will be performed on all apps.
                    Otherwise, the operation will be performed only on apps the method is called from.
                    Defaults to False.

            Returns:
                List[str]: Keys from local mount or remote object storage.
            """
            pass

        async def async_get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:  # noqa: E501
            """
            Get keys from local mount or remote object storage.

            Args:
                local (bool, optional): whether to use local mount or remote object storage.
                    If set to :code:`True`, the operation will
                    be performed on the local file system. Otherwise,
                    the operation will be performed on the remote object storage. Defaults to False.
                all_apps (bool, optional): whether to synchronize the operation between all apps.
                    If set to :code:`True`, the operation will be performed on all apps.
                    Otherwise, the operation will be performed only on apps the method is called from.
                    Defaults to False.

            Returns:
                List[str]: Keys from local mount or remote object storage.
            """
            pass

        def get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """
            Updates mount for this app (or all apps), return keys for which it was successful

            Args:
                keys (List[str]):
                    Keys by which values are obtained.
                    If this is not possible, this key will not be returned in result)
                force (bool, optional):
                    If set, it will ignore what is locally and
                    load data from the remote object storage.
                    Otherwise it will only take what does not exist.
                    Defaults to False.
                all_apps (bool, optional):
                    If set to true, the operation will be performed in all apps.
                    Otherwise only for apps with associated mount.
                    Defaults to True.

            Returns:
                List[str]: Keys by which it was possible
                to obtain the value and load it into the mount
            """
            pass

        async def async_get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """
            Updates mount for this app (or all apps), return keys for which it was successful

            Args:
                keys (List[str]):
                    Keys by which values are obtained.
                    If this is not possible, this key will not be returned in result)
                force (bool, optional):
                    If set, it will ignore what is locally and
                    load data from the remote object storage.
                    Otherwise it will only take what does not exist.
                    Defaults to False.
                all_apps (bool, optional):
                    If set to true, the operation will be performed in all apps.
                    Otherwise only for apps with associated mount.
                    Defaults to True.

            Returns:
                List[str]: Keys by which it was possible
                to obtain the value and load it into the mount
            """
            pass

        def get_all(
            self,
            local: bool = False,
            force: bool = False,
            all_apps: bool = True
        ) -> List[str]:
            """
            Updates mount and return all keys in it,
            if `local` - return result only for mount (or all apps mounts if `all_apps`),
            otherwise - load all by remote object storage

            Args:
                local (bool, optional): whether to use local mount or remote object storage.
                    If set to :code:`True`, the operation will
                    be performed on the local file system. Otherwise,
                    the operation will be performed on the remote object storage. Defaults to False.
                force (bool, optional):
                    If set, it will ignore what is locally and
                    load data from the remote object storage.
                    Otherwise it will only take what does not exist.
                    Defaults to False.
                all_apps (bool, optional):
                    If set to true, the operation will be performed in all apps.
                    Otherwise only for apps with associated mount.
                    Defaults to True.

            Returns:
                List[str]: All keys in the mount or all apps mounts if `all_apps` is True,
                otherwise load all keys from remote object storage.
            """
            pass

        async def async_get_all(
            self,
            local: bool = False,
            force: bool = False,
            all_apps: bool = True
        ) -> List[str]:
            """
            Updates mount and return all keys in it,
            if `local` - return result only for mount (or all apps mounts if `all_apps`),
            otherwise - load all by remote object storage

            Args:
                local (bool, optional): whether to use local mount or remote object storage.
                    If set to :code:`True`, the operation will
                    be performed on the local file system. Otherwise,
                    the operation will be performed on the remote object storage. Defaults to False.
                force (bool, optional):
                    If set, it will ignore what is locally and
                    load data from the remote object storage.
                    Otherwise it will only take what does not exist.
                    Defaults to False.
                all_apps (bool, optional):
                    If set to true, the operation will be performed in all apps.
                    Otherwise only for apps with associated mount.
                    Defaults to True.

            Returns:
                List[str]: All keys in the mount or all apps mounts if `all_apps` is True,
                otherwise load all keys from remote object storage.
            """
            pass

        def update(
            self,
            keys: List[str],
            presigned_expire: Optional[int] = -1
        ) -> Dict[str, str]:
            """Updates objects in remote storage

            Retrieves objects from local mount and updates remote object storage.
            If :code:`presigned_expire` is set to a positive value, creates
            and returns presigned urls for the objects.

            If :code:`presigned_expire` is None, set to default timeout.

            If :code:`presigned_expire` is negative, returns an empty dictionary.

            Args:
                keys (List[str]): Keys to update
                presigned_expire (int, optional):
                    If positve, life span of presigned urls in seconds.
                    If None, set to default timeout.
                    Defaults to -1.

            Returns:
                Dict[str, str]: Mapping of keys to presigned urls
            """
            pass

        async def async_update(
            self,
            keys: List[str],
            presigned_expire: Optional[int] = -1
        ) -> Dict[str, str]:
            """Updates objects in remote storage

            Retrieves objects from local mount and updates remote object storage.
            If :code:`presigned_expire` is set to a positive value, creates
            and returns presigned urls for the objects.

            If :code:`presigned_expire` is None, set to default timeout.

            If :code:`presigned_expire` is negative, returns an empty dictionary.

            Args:
                keys (List[str]): Keys to update
                presigned_expire (int, optional):
                    If positve, life span of presigned urls in seconds.
                    If None, set to default timeout.
                    Defaults to -1.

            Returns:
                Dict[str, str]: key to presigned url
            """
            pass

        def presigned(
            self,
            keys: List[str],
            expire: Optional[int] = None
        ) -> Dict[str, str]:
            """Creates presigned urls for specified keys

            Args:
                keys (List[str]): Keys to create presigned urls for
                expire (int, optional):
                    Life span of presigned urls in seconds (must be positive).
                    If None, set to default timeout.
                    Defaults to None.
            Returns:
                Dict[str, str]: Mapping of keys to presigned urls
            """
            pass

        async def async_presigned(
            self,
            keys: List[str],
            expire: Optional[int] = None
        ) -> Dict[str, str]:
            """Creates presigned urls for specified keys

            Args:
                keys (List[str]): Keys to create presigned urls for
                expire (int, optional):
                    Life span of presigned urls in seconds (must be positive).
                    If None, set to default timeout.
                    Defaults to None.
            Returns:
                Dict[str, str]: Mapping of keys to presigned urls
            """
            pass

        def delete(self, keys: List[str]) -> None:
            """Deletes values in mount and remote storage

            Args:
                keys (List[str]): Keys to delete
            """
            pass

        async def async_delete(self, keys: List[str]) -> None:
            """Deletes values in mount and remote storage

            Args:
                keys (List[str]): Keys to delete
            """
            pass

    app_id: str
    """
    App ID (unique for each app).
    This ID given to an app at startup by interpreters.
    """

    run_id: str
    """
    Run ID (unique for each run).
    """

    app_cfg: Dict[str, Any]
    """
    App configuration. Given to the app at startup
    and contains arbitrary configuration data. The configuration
    is preserved between runs. This field is used to
    dictate the behaviour of the app.
    """

    dag_key_value: _DagKeyValue
    """
    Key-value storage. Shared between all apps of one run.
    Values must be bytes, string, int or float; dictionary order is not guaranteed
    """

    object_storage: _ObjectStorage
    """
    Object storage, shared between all apps of one run.
    """

    common: Any
    """
    An object shared between all runs of the app.
    It is not being explicitly serialized, so may be any object.
    In comparison, objects put into :attr:`app_cfg` can be used
    in the same way, but they are serialized and deserialized limiting
    the types that can be used. That is not the case for :attr:`common`.
    """

    def __init__(self) -> None:
        self.app_id: str = ""                                       # app id at startup
        self.run_id: str = ""                                       # run id at startup
        self.app_cfg: Union[MinimalCfg, Dict[str, Any]] = {}        # configuration given to the app at startup  # noqa: E501
        self.msg_url: str = ""                                      # default url for msg operation              # noqa: E501
        self.email: Optional[str] = None                            # email for email_send operation             # noqa: E501
        self.dag_key_value = Context._DagKeyValue(
            self.run_id)      # key-value storage
        self.object_storage = Context._ObjectStorage()              # object storage
        self.common = None                                          # arbitrary common variable between app runs # noqa: E501
        self.logger = logging.getLogger(f"{self.operation_id}${self.run_id}")

    def share(
        self,
        path: str,
        all_runs: bool = False,
        path_prefix: str = APP_DIR,
        force: bool = False,
        synchronize: bool = True
    ) -> None:
        """Shares a file or a directory between all apps for the current deployment.

        The file or directory is copied to a shared directory, which is
        accessible by all apps. The file should be accessible by the
        following path:

            :code:`{path_prefix}/{path}`

        where :code:`path_prefix` and :code:`path` are the corresponding arguments.

        -----------------------------------------------------------------------------

        Note, that:

        *   The :code:`path_prefix` is not included into shared key.
        *   If :code:`all_runs` is set to :code:`True`,
            the file or directory will be shared between all runs of the app.
            Otherwise, it will be shared only between the apps of the current run.
        *   If :code:`synchronize` is set to :code:`True`,
            the file or directory will be copied to all apps.
            Otherwise, it will be copied only to apps with a common mount.
        *   If the file or directory already exists in the shared directory,
            it will not be copied unless :code:`force` is set to :code:`True`.

        -----------------------------------------------------------------------------

        .. note::

            See `Sharing files <#sharing-files>`_ for more details.

        Args:
            path (str):
                Key of shared file or directory
            all_runs (bool, optional):
                Whether to share the file or directory between all runs.
                Defaults to False.
            path_prefix (str, optional):
                Path prefix.
                Defaults to `APP_DIR`.
            force (bool, optional):
                Whether to overwrite the file or directory if it already exists.
                Defaults to False.
            synchronize (bool, optional):
                Whether to synchronize the file or directory between all apps.
                Defaults to True.
        """
        pass

    async def async_share(
        self,
        path: str,
        all_runs: bool = False,
        path_prefix: str = APP_DIR,
        force: bool = False,
        synchronize: bool = True
    ) -> None:
        """Shares a file or a directory between all apps for the current deployment.

        It is an asynchronous version of :meth:`share`.

        Args:
            path (str):
                Key of shared file or directory
            all_runs (bool, optional):
                Whether to share the file or directory between all runs.
                Defaults to False.
            path_prefix (str, optional):
                Path prefix.
                Defaults to `APP_DIR`.
            force (bool, optional):
                Whether to overwrite the file or directory if it already exists.
                Defaults to False.
            synchronize (bool, optional):
                Whether to synchronize the file or directory between all apps.
                Defaults to True.
        """
        pass

    def share_many(
        self,
        paths: List[str],
        all_runs: bool = False,
        path_prefix: str = APP_DIR,
        force: bool = False,
        synchronize: bool = True
    ) -> None:
        """Shares multiple files or directories.

        The same as :meth:`share`, but for multiple files or directories.
        Ignores paths that do not exist.
        """

    async def async_share_many(
        self,
        paths: List[str],
        all_runs: bool = False,
        path_prefix: str = APP_DIR,
        force: bool = False,
        synchronize: bool = True
    ) -> None:
        """Shares multiple files or directories.
        The same as :meth:`async_share`, but for multiple files or directories.
        Ignores paths that do not exist.
        """

    def get_share_path(
        self,
        path: str,
        all_runs: bool = False,
        not_exist_ok: bool = False
    ) -> str | None:
        """Retrieves a real file path by shared key.

        Once file is shared, it is copied to a shared directory, which is
        accessible by all apps. However, to actually access the file, you
        have to retrieve its real path using in the mounted file system.

        .. note::

            See `Accessing shared files <#accessing-shared-files>`_ for more details.


        Args:
            path (str): Shared file key
            all_runs (bool, optional):
                To access files shared between all runs, set to :code:`True`.
                Defaults to False.
            not_exist_ok (bool, optional):
                Whether to raise an exception if the file does not exist.

        Returns:
            str: Readable file path or None (if :code:`not_exist_ok` is set to :code:`True`)
        """
        pass

    def delete_share(
        self,
        path: str,
        all_runs: bool = False,
        synchronize: bool = True
    ) -> None:
        """Deletes previously shared file or directory.

        Args:
            path (str): Shared file key
            all_runs (bool, optional):
                Whether to delete files shared between all runs.
                Defaults to False.
            synchronize (bool, optional):
                Whether to synchronize the deletion between all apps.
                Defaults to True.
        """
        pass

    async def async_delete_share(
        self, path: str, all_runs: bool = False, synchronize: bool = True
    ) -> None:
        """Deletes previously shared file or directory.

        Args:
            path (str): Shared file key
            all_runs (bool, optional):
                Whether to delete files shared between all runs.
                Defaults to False.
            synchronize (bool, optional):
                Whether to synchronize the deletion between all apps.
                Defaults to True.
        """
        pass

    def synchronize(
        self,
        paths: Optional[List[str]] = None,
        all_runs: bool = False
    ) -> None:  # TODO synchronize removing
        """Forcefully synchronizes paths accross all apps.

        Setting :code:`paths` to :code:`None` or an empty list will synchronize
        all shared files and directories.

        Args:
            paths (Optional[List[str]], optional): Paths to synchronize.
            all_runs (bool, optional):
                Whether to synchronize files shared between all runs.
                Should be set to the same value as :meth:`share` method was called with.
                Defaults to False.
        """
        pass

    async def async_synchronize(
        self, paths: Optional[List[str]] = None, all_runs: bool = False
    ) -> None:
        """Forcefully synchronizes paths accross all apps.

        Setting :code:`paths` to :code:`None` or an empty list will synchronize
        all shared files and directories.

        Args:
            paths (Optional[List[str]], optional): Paths to synchronize.
            all_runs (bool, optional):
                Whether to synchronize files shared between all runs.
                Should be set to the same value as :meth:`share` method was called with.
                Defaults to False.
        """
        pass

    def msg(
        self,
        data: Union[str, Dict],
        url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        wait: bool = False,
        wrap: bool = True,
        with_result: bool = False
    ) -> None:
        """Sends http msg to system or any url

        Args:
            data (Union[str, Dict]): msg data
            url (Optional[str]): url to which the request is sent. If not specified, the default url is used (system url, but it can be overridden in the startup cfg, the parameter is msg_url). system url. Defaults to None.
            headers (Optional[Dict[str, str]]): Just headers, with which the request is sent. Defaults to None.
            wait (bool): wait result. Defaults to False.
            wrap (bool): need system wrap with operationId. Defaults to True.
            with_result (bool): return result. Defaults to False.
        """  # noqa: E501
        pass

    async def async_msg(self, data: Union[str, Dict], url: Optional[str] = None, headers: Optional[Dict[str, str]] = None, wait: bool = False, wrap: bool = True, with_result: bool = False):  # noqa: ANN201, E501
        """Sens http msg to system or any url

        Args:
            data (Union[str, Dict]): msg data
            url (Optional[str]): url to which the request is sent. If not specified, the default url is used (system url, but it can be overridden in the startup cfg, the parameter is msg_url). system url. Defaults to None.
            headers (Optional[Dict[str, str]]): Just headers, with which the request is sent. Defaults to None.
            wait (bool): wait result. Defaults to False.
            wrap (bool): need system wrap with operationId. Defaults to True.
            with_result (bool): return result. Defaults to False.
        """  # noqa: E501
        pass

    def email_send(self, message: str, subject: Optional[str] = None, type: str = "gmail") -> None:  # noqa: E501
        """Send an email

        Args:
            message (str): text message
            subject (Optional[str], optional): message subject, if subject is None used default. Defaults to None.
            type (str, optional): message type, only gmail work now. Defaults to "gmail".
        """  # noqa: E501
        pass

    def metadata(self, df_name: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:  # noqa: E501
        """Gets metadata by df_name (if it saved with collection)

        Args:
            df_name (str): df name

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: metadata if exists (list if many), None otherwise
        """  # noqa: E501
        pass

    @property
    def scale_info(self) -> Tuple[int, int]:
        """Gets scale info: `index` and `index count`. `index count` - how many `apps` run it, `index` in [0, `index count`)

        Returns:
            Tuple[int, int]: `index` and `index count`
        """  # noqa: E501
        pass

    def get_scale_part(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gets scale part of df (`index` and `index count` used for that) - all apps app get different data

        Args:
            df (pd.DataFrame): df to scale

        Returns:
            pd.DataFrame: scale part of df
        """  # noqa: E501
        pass

    @property
    def operation_id(self) -> str:
        """Operation identifier

        Returns:
            str: operation_id
        """
        pass

    def has_object(self, path: str) -> bool:
        """Checks whether there is an asset (OBJ) at the path

        Args:
            path (str): Path to object (asset)

        Returns:
            bool: exist or not
        """
        pass

    def get_object(self, path: str) -> OBJ:
        """Composes a path to an asset (OBJ) and returns it as :class:`OBJ`

        If the asset (object) does not exist, the function
        fails with an error.

        Args:
            path (str): Path to object (asset)

        Returns:
            OBJ: An OBJ wrap around a path
        """
        pass


    def as_object(
        self,
        paths: Dict[str, str],
        path_prefix: Optional[str] = None,
        *,
        dir: Optional[str] = None,
        allow_update_dir: bool = True
    ) -> OBJ:
        """Creates an asset (OBJ) by copying paths to specific directory and creating :class:`OBJ` by this dir.

        Assets (:class:`OBJ`) are simply a path within a directory accessible from within container
        by a certain user. This function creates a new asset by copying specified files into separate directory
        and creating an asset pointing to whole folder.

        Args:
            paths (Dict[str, str]): Path to an actual object -> subpath in asset directory
            path_prefix (Optional[str], optional): prefix for `paths` key if not None
            dir (Optional[str], optional): target directory name. If not set, it is generated. Defaults to None
            allow_update_dir (bool): raise exception if `dir` already exist and it set to False

        Returns:
            OBJ: OBJ with created directory captured
        """ # noqa: E501
        pass


def to_binary(smth: Any) -> bytes:  # noqa: ANN401
    """Converts object to binary
<<<<<<< HEAD
=======

>>>>>>> f9049b55f1efeadd27e32843d9c0d8c4431a7405
    Args:
        smth (Any): object to convert
    """
    return pickle.dumps(smth)


def from_binary(smth: bytes) -> Any:
    """Converts binary to object
<<<<<<< HEAD
=======

>>>>>>> f9049b55f1efeadd27e32843d9c0d8c4431a7405
    Args:
        smth (bytes): binary to convert
    """
    return pickle.loads(smth)


def load(url: str, path: str, path_prefix: str = APP_DIR) -> None:
    """Get request url and save result to path

    Args:
        url (str): url to load
        path (str): relative path - starting from the `path_prefix` (apps directory in app (`APP_DIR`) by default)
        path_prefix (str, optional): prefix for the path. Defaults to APP_DIR.
    """  # noqa: E501
    pass


class S3Helper:
    """
    Ready-made auxiliary wrapper for interacting with custom s3
    """

    def __init__(self, client: Any, s3_bucket: str) -> None:  # noqa: ANN401
        self.client = client
        self.__bucket = s3_bucket

    @staticmethod
    def create_by_cfg(cfg: Dict[str, Any], **kwargs) -> 'S3Helper':
        s3_client = boto3.client(
            's3',
            region_name=cfg.get('aws_region'),
            aws_access_key_id=cfg['aws_access_key_id'],
            aws_secret_access_key=cfg['aws_secret_access_key'],
            endpoint_url=cfg.get('endpoint_url'),
            **kwargs
        )
        return S3Helper(s3_client, cfg['s3_bucket'])

    def get_object(self, key: str, bucket: Optional[str] = None) -> Optional[StreamingBody]:  # noqa: E501
        """Uses :code:`get_object` from client by `bucket` and `key`

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.

        Returns:
            Optional[StreamingBody]: get_object response body
        """  # noqa: E501
        pass

    def get_df(self, key: str, bucket: Optional[str] = None) -> pd.DataFrame:
        """Uses :code:`get_object` result and cast it to data frame

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.

        Returns:
            pd.DataFrame: result in df view
        """  # noqa: E501
        pass

    def save_object(self, body: Any, key: str, bucket: Optional[str] = None) -> None:  # noqa: ANN401
        """Uses :code:`put_object` from client by `bucket`, `key` and `body`

        Args:
            body (Any): saved data
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """  # noqa: E501
        pass

    def save_df(self, df: pd.DataFrame, key: str, bucket: Optional[str] = None) -> None:
        """Uses :code:`save_object` to save df by `bucket` and `key`

        Args:
            df (pd.DataFrame): df to save
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """  # noqa: E501
        pass

    def delete_object(self, key: str, bucket: Optional[str] = None) -> None:
        """Deletes object by `bucket` and `key`

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """  # noqa: E501
        pass


class SmtpSender:
    """
    Ready-made auxiliary wrapper for interacting with SMTP
    Args:
        login (str): login
        password (str): password
        smtp_server (str, optional): smtp server. Defaults to "smtp.gmail.com".
        smtp_port (int, optional): smtp port. Defaults to 465.
    """

    def __init__(self, login: str, password: str, smtp_server: str = "smtp.gmail.com", smtp_port: int = 465) -> None:  # noqa: E501
        self.server = smtp_server
        self.port = smtp_port
        self.login = login
        self.password = password

    def send(self, receivers: list[str], subject: str, message: str) -> None:
        """Sends an email

        Args:
            receivers (list[str]): list of emails
            subject (str): message subject
            message (str): message text
        """
        pass


_Tensor = TypeVar('_Tensor', bound='torch.Tensor')
_kshort = 0b111  # last two-byte char encodes <= 7 bits
_kexclude_idx = {chr(0): 0, chr(10): 1, chr(
    13): 2, chr(34): 3, chr(38): 4, chr(92): 5}
_idx_exclude = {0: chr(0), 1: chr(10), 2: chr(
    13), 3: chr(34), 4: chr(38), 5: chr(92)}


def _base_encode(data: bytes) -> str:
    idx = (bit := 0)

    def get7(length):
        """get 7 bits from data"""
        nonlocal idx, bit, data
        if idx >= length:
            return False, 0

        # AND mask to get the first 7 bits
        f_ = (((0b11111110 % 0x100000000) >> bit) & data[idx]) << bit
        f_ = f_ >> 1
        bit += 7
        if bit < 8:
            return True, f_
        bit -= 8
        idx += 1
        if idx >= length:
            return True, f_
        secondPart = (((0xFF00 % 0x100000000) >> bit) & data[idx]) & 0xFF
        secondPart = secondPart >> (8 - bit)
        return True, f_ | secondPart

    _out = bytearray()
    while True:
        rbits, bits = get7(len(data))
        if not rbits:
            break
        if bits in _kexclude_idx:
            illegalIndex = _kexclude_idx[bits]
        else:
            _out.append(bits)
            continue
        retNext, nextBits = get7(len(data))
        b1 = 0b11000010
        b2 = 0b10000000
        if not retNext:
            b1 |= (0b111 & _kshort) << 2
            nextBits = bits
        else:
            b1 |= (0b111 & illegalIndex) << 2
        firstBit = 1 if (nextBits & 0b01000000) > 0 else 0
        b1 |= firstBit
        b2 |= nextBits & 0b00111111
        _out += [b1, b2]
    return ''.join([chr(x) for x in _out])


def _base_decode(encoded_data: str) -> bytes:
    encoded_data = [ord(x) for x in encoded_data]
    decoded = []
    curByte = bitOfByte = 0

    def push7(byte):
        nonlocal curByte, bitOfByte, decoded
        byte <<= 1
        curByte |= (byte % 0x100000000) >> bitOfByte
        bitOfByte += 7
        if bitOfByte >= 8:
            decoded += [curByte]
            bitOfByte -= 8
            curByte = (byte << (7 - bitOfByte)) & 255
        return

    for i in range(len(encoded_data)):
        if encoded_data[i] > 127:
            illegalIndex = ((encoded_data[i] % 0x100000000) >> 8) & 7
            if illegalIndex != _kshort:
                push7(_idx_exclude[illegalIndex])
            push7(encoded_data[i] & 127)
        else:
            push7(encoded_data[i])
    return bytearray(decoded)


def _tensor_to_df(x: list[_Tensor] | _Tensor) -> pd.DataFrame:
    import io

    import torch  # not in requirements

    if not isinstance(x, list):
        x = [x]

    shapes = []
    data = []
    grads = []
    device = []
    for x_ in x:
        assert isinstance(x_, torch.Tensor), f"not a tensor: {type(x_)}"
        # in-memory serialization using torch.save
        # to save autograd information and tensor type
        # https://pytorch.org/docs/stable/torch.html#torch.save
        buff = io.BytesIO()
        shape = x_.shape
        shapes.append(shape)
        device.append(x_.device)
        x_ = x_.cpu()
        torch.save(x_, buff)
        buff.seek(0)
        data.append(_base_encode(buff.read()))

        buff.close()
        buff = io.BytesIO()
        torch.save(x_.grad, buff)
        buff.seek(0)

        grads.append(_base_encode(buff.read()))

    return pd.DataFrame(
        {
            "__shape__": shapes,
            "__tensor__": data,
            "__grad__": grads,
            "__device__": device,
        })


def _tensor_from_df(x: pd.DataFrame) -> list:
    import io

    import torch  # not in requirements

    _out = []
    for _, row in x.iterrows():
        shape = row["__shape__"]
        encoded = row["__tensor__"]
        encoded_grad = row["__grad__"]
        decoded = _base_decode(encoded)
        decoded_grad = _base_decode(encoded_grad)
        buff = io.BytesIO(decoded)
        buff.seek(0)
        _t = torch.load(buff).reshape(shape)

        if _t.requires_grad:
            buff.close()

            buff = io.BytesIO(decoded_grad)
            buff.seek(0)
            _t.grad = torch.load(buff).reshape(shape)
            buff.close()

        if row["__device__"] != "cpu" and torch.cuda.is_available():
            _t = _t.to(row["__device__"])

        _out.append(_t)

    return _out


def to_df(x: Any, force: bool = False) -> pd.DataFrame:
    """Creates a data frame from an arbitrary object
    - `torch.Tensor`: Tensor is serialized using torch.save and then encoded using base112. Autograd information is preserved.
    - `numpy`, `list`, `tuple`, `range`, `bytearray`: Data is serialized using pickle and stored as is in `data` column.
    - `set`, `frozenset`: Data is converted to list and stored as is in `data` column.
    - `dict`: Data is serialized using json and stored as is in `data` column.
    - `int`, float, complex, str, bytes, bool: Data is stored as is in `data` column.


    Args:
        x (Any): Object to convert to data frame
        force (bool, optional):
            If set, it will ignore the type of the object and serialize it using pickle.
            Defaults to False.

    Returns:
        pd.DataFrame: Data frame with a single column :code:`data`
    """ # noqa: E501
    if force:
        return pd.DataFrame({"data": [jsonpickle.encode(x)]})
    elif type(x).__name__ == "Tensor" or (isinstance(x, list) and len(x) > 0 and type(x[0]).__name__ == "Tensor"):
        return _tensor_to_df(x)
    elif isinstance(x, (np.ndarray, list, tuple, range, bytearray)):
        return pd.DataFrame({"data": x})
    elif isinstance(x, (set, frozenset)):
        return pd.DataFrame({"data": list(x)})
    elif isinstance(x, dict):
        return pd.DataFrame({"data": [json.dumps(x)]})
    else:   # int, float, complex, str, bytes, bool
        return pd.DataFrame({"data": [x]})


# TODO create same with pyspark
def from_df(x: pd.DataFrame, type_name: Optional[str] = None, force: bool = False) -> Any:  # noqa: ANN401, E501
    """Converts a data frame obtained by running :func:`to_df` back to an object

    Args:
        x (pd.DataFrame): Data frame to convert
        type_name (Optional[str], optional):
            Type of the object to convert to. If not specified, the type is inferred from the data frame.
            Defaults to None.
        force (bool, optional):
            If set, it will ignore the type of the object and deserialize it using pickle.
            Defaults to False.

    Returns:
        Any: Object of type :code:`type_name` or inferred type
    """
    if force:
        return jsonpickle.decode(x.data[0])
    elif type_name == 'ndarray':
        return x.data.values
    elif type_name == 'list':
        return x.data.values.tolist()
    elif type_name == 'tuple':
        return tuple(x.data.values.tolist())
    elif type_name == 'range':
        return x.data.values.tolist()
    elif type_name == 'Tensor' or ('__shape__' in x.columns and '__tensor__' in x.columns):
        # import torch  # not in requirements
        # return torch.from_numpy(x.values).float().to(torch.device('cpu'))   # can't work with gpu from inside yet  # noqa: E501
        return _tensor_from_df(x)
    elif type_name == 'set':
        return set(x.data.values.tolist())
    elif type_name == 'frozenset':
        return frozenset(x.data.values.tolist())
    elif type_name == 'dict':
        return json.loads(x.data[0])
    elif type_name == 'bytearray':
        return bytearray(x.data)
    else:   # int, float, complex, str, bytes, bool
        return x.data[0]
