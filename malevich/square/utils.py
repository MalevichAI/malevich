import json
import logging
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import jsonpickle
import numpy as np
import pandas as pd
from botocore.response import StreamingBody

WORKDIR = "/julius"         # docker workdir    # FIXME "/malevich"
APP_DIR = f"{WORKDIR}/apps" # dir into which the user code is copied


class Context:
    class _DagKeyValue:
        """key-value storage, shared for all apps of one run\n
        values must be bytes, string, int or float; dictionary order is not guaranteed
        """
        def __init__(self, run_id: Optional[str] = None) -> None:
            pass

        def get_bytes(self, key: str) -> bytes:
            """a more optimal way to get a binary value by key (`get` can also be used)

            Args:
                key (str): key in storage

            Returns:
                bytes: value by key
            """
            pass

        async def async_get_bytes(self, key: str) -> bytes:
            """a more optimal way to get a binary value by key (`get` can also be used)

            Args:
                key (str): key in storage

            Returns:
                bytes: value by key
            """
            pass

        def get(self, keys: List[str]) -> Dict[str, Any]:
            """return key -> values by keys

            Args:
                keys (List[str]): list of keys

            Returns:
                Dict[str, Any]: dict key -> value, if there is no value, it will be None
            """
            pass

        async def async_get(self, keys: List[str]) -> Dict[str, Any]:
            """return key -> values by keys

            Args:
                keys (List[str]): list of keys

            Returns:
                Dict[str, Any]: dict key -> value, if there is no value, it will be None
            """
            pass

        def get_all(self) -> Dict[str, Any]:
            """return key -> value with all stored values

            Returns:
                Dict[str, Any]: dict key -> value
            """
            pass

        async def async_get_all(self) -> Dict[str, Any]:
            """return key -> value with all stored values

            Returns:
                Dict[str, Any]: dict key -> value
            """
            pass

        def update(self, keys_values: Dict[str, Any]) -> None:
            """sets the value by key by dict key -> value, if it has already been set - overwrites it

            Args:
                keys_values (Dict[str, Any]): dict key -> value
            """ # noqa: E501
            pass

        async def async_update(self, keys_values: Dict[str, Any]) -> None:
            """sets the value by key by dict key -> value, if it has already been set - overwrites it

            Args:
                keys_values (Dict[str, Any]): dict key -> value
            """  # noqa: E501
            pass

        def clear(self) -> None:
            """deletes everything saved
            """
            pass

        async def async_clear(self) -> None:
            """deletes everything saved
            """
            pass

    class _ObjectStorage:
        """object storage, works through the shared part of the app file system - mount and with `s3`.\n
        it is common to the user, i.e. can be used between different runs
        for the operation you need to put or take the result from it - `share`/`get_share_path`.\n
        in any case, the mount may be in an unsynchronized state for some apps; therefore, in many functions there is `all_apps`, which guarantees behavior as if everything was synchronized - the operation is applied to all mounts.\n
        it can work in two ways - locally and with a remote part - there is a `local` parameter for this. If you run with it, then everything will work with mount, otherwise with remote object storage.
        """ # noqa: E501
        def get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:
            """get keys from local mount or remote object storage

            Args:
                local (bool, optional): use for this operation mount (True) or remote object storage (False). Defaults to False.
                all_apps (bool, optional): get result by all apps (if you need results from other apps and the mount is not synchronized between them). Defaults to False.

            Returns:
                List[str]: keys from local mount or remote object storage
            """ # noqa: E501
            pass

        async def async_get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:  # noqa: E501
            """get keys from local mount or remote object storage

            Args:
                local (bool, optional): use for this operation mount (True) or remote object storage (False). Defaults to False.
                all_apps (bool, optional): get result by all apps (if you need results from other apps and the mount is not synchronized between them). Defaults to False.

            Returns:
                List[str]: keys from local mount or remote object storage
            """ # noqa: E501
            pass

        def get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """update mount for this app (or all apps if all_apps=True), return keys for whom it was possible and happened

            Args:
                keys (List[str]): keys by which values are obtained (if this is not possible, this key will not be returned in result)
                force (bool, optional): if installed, it will ignore what is locally and load data from the remote object storage, otherwise it will only take what does not exist. Defaults to False.
                all_apps (bool, optional): do it operation in all apps, otherwise only for apps with associated mount. Defaults to True.

            Returns:
                List[str]: keys by which it was possible to obtain the value and load it into the mount
            """ # noqa: E501
            pass

        async def async_get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """update mount for this app (or all apps if all_apps=True), return keys for whom it was possible and happened

            Args:
                keys (List[str]): keys by which values are obtained (if this is not possible, this key will not be returned in result)
                force (bool, optional): if installed, it will ignore what is locally and load data from the remote object storage, otherwise it will only take what does not exist. Defaults to False.
                all_apps (bool, optional): do it operation in all apps, otherwise only for apps with associated mount. Defaults to True.

            Returns:
                List[str]: keys by which it was possible to obtain the value and load it into the mount
            """ # noqa: E501
            pass

        def get_all(self, local: bool = False, force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """update mount and return all keys in it, if `local` - return result only for mount (or all apps mounts if `all_apps`), otherwise - load all by remote object storage

            Args:
                local (bool, optional): interacts only with the local part. Defaults to False.
                force (bool, optional): if installed, it will ignore what is locally and load data from the remote object storage, otherwise it will only take what does not exist. Defaults to False.
                all_apps (bool, optional): do it operation in all apps, otherwise only for apps with associated mount. Defaults to True.

            Returns:
                List[str]: all keys in mount, that loaded
            """ # noqa: E501
            pass

        async def async_get_all(self, local: bool = False, force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            """update mount and return all keys in it, if `local` - return result only for mount (or all apps mounts if `all_apps`), otherwise - load all by remote object storage

            Args:
                local (bool, optional): interacts only with the local part. Defaults to False.
                force (bool, optional): if installed, it will ignore what is locally and load data from the remote object storage, otherwise it will only take what does not exist. Defaults to False.
                all_apps (bool, optional): do it operation in all apps, otherwise only for apps with associated mount. Defaults to True.

            Returns:
                List[str]: all keys in mount, that loaded
            """ # noqa: E501
            pass

        def update(self, keys: List[str], presigned_expire: Optional[int] = -1) -> Dict[str, str]: # noqa: E501
            """update remote object storage by this keys, values should be in local mount for this keys; maybe create presigned url

            Args:
                keys (List[str]): keys, for which it is updated remote object storage
                presigned_expire (Optional[int]): if < 0 - do nothing, otherwise create presigned url (value in seconds, None - default value). Defaults to -1.

            Returns:
                Dict[str, str]: key to presigned url
            """ # noqa: E501
            pass

        async def async_update(self, keys: List[str]) -> None:
            """update remote object storage by this keys, values should be in local mount for this keys; maybe create presigned url

            Args:
                keys (List[str]): keys, for which it is updated remote object storage
                presigned_expire (Optional[int]): if < 0 - do nothing, otherwise create presigned url (value in seconds, None - default value). Defaults to -1.

            Returns:
                Dict[str, str]: key to presigned url
            """ # noqa: E501
            pass

        def presigned(self, keys: List[str], expire: Optional[int] = None) -> Dict[str, str]: # noqa: E501
            """create presigned url for keys

            Args:
                keys (List[str]): keys, for which it is updated remote object storage
                expire (Optional[int], optional): should be > 0; create presigned url (value in seconds, None - default value). Defaults to None.

            Returns:
                Dict[str, str]: key to presigned url
            """ # noqa: E501
            pass

        async def async_presigned(self, keys: List[str], expire: Optional[int] = None) -> Dict[str, str]: # noqa: E501
            """create presigned url for keys

            Args:
                keys (List[str]): keys, for which it is updated remote object storage
                expire (Optional[int], optional): should be > 0; create presigned url (value in seconds, None - default value). Defaults to None.

            Returns:
                Dict[str, str]: key to presigned url
            """ # noqa: E501
            pass

        def delete(self, keys: List[str]) -> None:
            """delete values in mount and remote storage for this keys

            Args:
                keys (List[str]): keys for which you need to delete from object storage
            """
            pass

        async def async_delete(self, keys: List[str]) -> None:
            """delete values in mount and remote storage for this keys

            Args:
                keys (List[str]): keys for which you need to delete from object storage
            """
            pass

    """context for run"""

    def __init__(self) -> None:
        self.app_id: str = ""                                       # app id at startup
        self.run_id: str = ""                                       # run id at startup
        self.app_cfg: Dict[str, Any] = {}                           # configuration given to the app at startup  # noqa: E501
        self.msg_url: str = ""                                      # default url for msg operation              # noqa: E501
        self.email: Optional[str] = None                            # email for email_send operation             # noqa: E501
        self.dag_key_value = Context._DagKeyValue(self.run_id)      # key-value storage
        self.object_storage = Context._ObjectStorage()              # object storage
        self.common = None                                          # arbitrary common variable between app runs # noqa: E501
        self.logger = logging.getLogger(f"{self.operation_id}${self.run_id}")

    def share(self, path: str, all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """copy dir (if it doesn't already exist or `force=True`) or file along the path starting from the `path_prefix` (apps directory in app (`APP_DIR`) by default) to the shared directory for all apps

        Args:
            path (str): path to share, the "real path" by which it is stored
            all_runs (bool, optional): share it for all runs, otherwise for all - this value should be the same in `get_share_path`. Defaults to False.
            path_prefix (str, optional): prefix for the path (specifies the location when loading and is no longer used). Defaults to APP_DIR.
            force (bool, optional): remove share if it exist. Defaults to False.
            synchronize (bool, optional): synchronize across all apps, otherwise some apps with a common mount will be synchronized. Defaults to True.
        """ # noqa: E501
        pass

    async def async_share(self, path: str, all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """copy dir (if it doesn't already exist or `force=True`) or file along the path starting from the `path_prefix` (apps directory in app (`APP_DIR`) by default) to the shared directory for all apps

        Args:
            path (str): path to share, the "real path" by which it is stored
            all_runs (bool, optional): share it for all runs, otherwise for all - this value should be the same in `get_share_path`. Defaults to False.
            path_prefix (str, optional): prefix for the path (specifies the location when loading and is no longer used). Defaults to APP_DIR.
            force (bool, optional): remove share if it exist. Defaults to False.
            synchronize (bool, optional): synchronize across all apps, otherwise some apps with a common mount will be synchronized. Defaults to True.
        """ # noqa: E501
        pass

    def share_many(self, paths: List[str], all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """same as `share` but for multiple paths, ignore not exists path
        """

    async def async_share_many(self, paths: List[str], all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """same as `share` but for multiple paths, ignore not exists path
        """

    def get_share_path(self, path: str, all_runs: bool = False, not_exist_ok: bool = False) -> str:  # noqa: E501
        """return real path by `path`, that shared before with function `share`

        Args:
            path (str): path, with which share was called earlier
            all_runs (bool, optional): value, with which share was called earlier. Defaults to False.
            not_exist_ok (bool, optional): throw if path not exist if False. Defaults to False.

        Returns:
            str: real path
        """ # noqa: E501
        pass

    def delete_share(self, path: str, all_runs: bool = False, synchronize: bool = True) -> None:    # noqa: E501
        """delete dir or file, that shared between all apps, `path` like path is the same as used in function `share`

        Args:
            path (str): path, with which share was called earlier
            all_runs (bool, optional): value, with which share was called earlier. Defaults to False.
            synchronize (bool, optional): synchronize for all mounts. Defaults to True.
        """ # noqa: E501
        pass

    async def async_delete_share(self, path: str, all_runs: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """delete dir or file, that shared between all apps, `path` like path is the same as used in function `share`

        Args:
            path (str): path, with which share was called earlier
            all_runs (bool, optional): value, with which share was called earlier. Defaults to False.
            synchronize (bool, optional): synchronize for all mounts. Defaults to True.
        """ # noqa: E501
        pass

    def synchronize(self, paths: Optional[List[str]] = None, all_runs: bool = False) -> None:  # TODO synchronize removing  # noqa: E501
        """synchronize all mounts,

        Args:
            paths (Optional[List[str]], optional): paths to synchronize, if paths = None or [] - synchronize from root mount. Defaults to None.
            all_runs (bool, optional): value, with which share was called earlier. Defaults to False.
        """ # noqa: E501
        pass

    async def async_synchronize(self, paths: Optional[List[str]] = None, all_runs: bool = False) -> None:  # noqa: E501
        """synchronize all mounts,

        Args:
            paths (Optional[List[str]], optional): paths to synchronize, if paths = None or [] - synchronize from root mount. Defaults to None.
            all_runs (bool, optional): value, with which share was called earlier. Defaults to False.
        """ # noqa: E501
        pass

    def msg(self, data: Union[str, Dict], url: Optional[str] = None, headers: Optional[Dict[str, str]] = None, wait: bool = False, wrap: bool = True, with_result: bool = False):  # noqa: E501, ANN201
        """send http msg to system or any url

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
        """send http msg to system or any url

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
        """send email

        Args:
            message (str): text message
            subject (Optional[str], optional): message subject, if subject is None used default. Defaults to None.
            type (str, optional): message type, only gmail work now. Defaults to "gmail".
        """ # noqa: E501
        pass

    def metadata(self, df_name: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:  # noqa: E501
        """get metadata by df_name (if it saved with collection)

        Args:
            df_name (str): df name

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: metadata if exists (list if many), None otherwise
        """ # noqa: E501
        pass

    @property
    def scale_info(self) -> Tuple[int, int]:
        """get scale info: `index` and `index count`. `index count` - how many `apps` run it, `index` in [0, `index count`)

        Returns:
            Tuple[int, int]: `index` and `index count`
        """ # noqa: E501
        pass

    def get_scale_part(self, df: pd.DataFrame) -> pd.DataFrame:
        """get scale part of df (`index` and `index count` used for that) - any app get different data

        Args:
            df (pd.DataFrame): df to scale

        Returns:
            pd.DataFrame: scale part of df
        """ # noqa: E501
        pass

    @property
    def operation_id(self) -> str:
        """return operation_id

        Returns:
            str: operation_id
        """
        pass


def to_binary(smth: Any) -> bytes:  # noqa: ANN401
    return pickle.dumps(smth)


def from_binary(smth: bytes) -> Any:  # noqa: ANN401
    return pickle.loads(smth)


def load(url: str, path: str, path_prefix: str = APP_DIR) -> None:
    """get request url and save result to path

    Args:
        url (str): url to load
        path (str): relative path - starting from the `path_prefix` (apps directory in app (`APP_DIR`) by default)
        path_prefix (str, optional): prefix for the path. Defaults to APP_DIR.
    """ # noqa: E501
    pass


class S3Helper:
    """ready-made auxiliary wrapper for interacting with custom s3
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
        """use get_object from client by `bucket` and `key`

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.

        Returns:
            Optional[StreamingBody]: get_object response body
        """ # noqa: E501
        pass

    def get_df(self, key: str, bucket: Optional[str] = None) -> pd.DataFrame:
        """use get_object result and cast it to df

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.

        Returns:
            pd.DataFrame: result in df view
        """ # noqa: E501
        pass

    def save_object(self, body: Any, key: str, bucket: Optional[str] = None) -> None:  # noqa: ANN401
        """use put_object from client by `bucket`, `key` and `body`

        Args:
            body (Any): saved data
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """ # noqa: E501
        pass

    def save_df(self, df: pd.DataFrame, key: str, bucket: Optional[str] = None) -> None:
        """use save_object to save df by `bucket` and `key`

        Args:
            df (pd.DataFrame): df to save
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """ # noqa: E501
        pass

    def delete_object(self, key: str, bucket: Optional[str] = None) -> None:
        """delete object by `bucket` and `key`

        Args:
            key (str): object storage key
            bucket (Optional[str], optional): object storage bucket, if set - override default. Defaults to None.
        """ # noqa: E501
        pass


class SmtpSender:
    """ready-made auxiliary wrapper for interacting with smtp
    """
    def __init__(self, login: str, password: str, smtp_server: str = "smtp.gmail.com", smtp_port: int = 465) -> None:  # noqa: E501
        self.server = smtp_server
        self.port = smtp_port
        self.login = login
        self.password = password

    def send(self, receivers: list[str], subject: str, message: str) -> None:
        """send message

        Args:
            receivers (list[str]): list of emails
            subject (str): message subject
            message (str): message text
        """
        pass


def to_df(x: Any, force: bool = False) -> pd.DataFrame:  # noqa: ANN401
    """creates a dataframe in a certain way, `force` should be used for complex objects ('ndarray', 'Tensor' and python primitives work without it. It crashes on basic types ('int', 'str', etc)), scheme of received dataframe - `default_scheme`"""  # noqa: E501
    if force:
        return pd.DataFrame({"data": [jsonpickle.encode(x)]})
    elif isinstance(x, (np.ndarray, list, tuple, range, bytearray)):
        return pd.DataFrame({"data": x})
    elif type(x).__name__ == "Tensor":
        return pd.DataFrame(x)
    elif isinstance(x, (set, frozenset)):
        return pd.DataFrame({"data": list(x)})
    elif isinstance(x, dict):
        return pd.DataFrame({"data": [json.dumps(x)]})
    else:   # int, float, complex, str, bytes, bool
        return pd.DataFrame({"data": [x]})


# TODO create same with pyspark
def from_df(x: pd.DataFrame, type_name: Optional[str] = None, force: bool = False) -> Any: # noqa: ANN401, E501
    """decodes the `to_df` data from the dataframe, `force` is used if it was used in the encoding function - `to_df`. You should specify the type (by type_name: for example 'ndarray', 'list', 'Tensor', 'int') that was put in this `to_df` dataframe.
    possible type_names: 'ndarray', 'list', 'tuple', 'Tensor', 'set', 'frozenset', 'dict', 'bytearray'. Otherwise considered a primitive base type
    if force==True ignore type_name anyway"""  # noqa: E501
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
    elif type_name == 'Tensor':
        import torch  # not in requirements
        return torch.from_numpy(x.values).float().to(torch.device('cpu'))   # can't work with gpu from inside yet  # noqa: E501
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
