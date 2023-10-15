import json
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import jsonpickle
import numpy as np
import pandas as pd
from botocore.response import StreamingBody

WORKDIR = "/julius" # FIXME "/malevich"
APP_DIR = f"{WORKDIR}/apps"


class Context:
    class _DagKeyValue:
        """values must be bytes, string, int or float; dictionary order is not guaranteed"""  # noqa: E501
        def __init__(self, run_id: Optional[str] = None) -> None:
            pass

        def get_bytes(self, key: str) -> bytes:
            """a more optimal way to get a binary value by key (\"get\" can also be used)"""    # noqa: E501
            pass

        async def async_get_bytes(self, key: str) -> bytes:
            """a more optimal way to get a binary value by key (\"get\" can also be used)"""    # noqa: E501
            pass

        def get(self, keys: List[str]) -> Dict[str, Any]:
            pass

        async def async_get(self, keys: List[str]) -> Dict[str, Any]:
            pass

        def get_all(self) -> Dict[str, Any]:
            pass

        async def async_get_all(self) -> Dict[str, Any]:
            pass

        def update(self, keys_values: Dict[str, Any]) -> None:
            pass

        async def async_update(self, keys_values: Dict[str, Any]) -> None:
            pass

        def clear(self) -> None:
            pass

        async def async_clear(self) -> None:
            pass

    class _ObjectStorage:
        def get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:
            pass

        async def async_get_keys(self, local: bool = False, all_apps: bool = False) -> List[str]:  # noqa: E501
            pass

        def get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            pass

        async def async_get(self, keys: List[str], force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            pass

        def get_all(self, local: bool = False, force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            pass

        async def async_get_all(self, local: bool = False, force: bool = False, all_apps: bool = True) -> List[str]:    # noqa: E501
            pass

        def update(self, keys: List[str]) -> None:
            pass

        async def async_update(self, keys: List[str]) -> None:
            pass

        def delete(self, keys: List[str]) -> None:
            pass

        async def async_delete(self, keys: List[str]) -> None:
            pass

    """context for run"""
    def __init__(self) -> None:
        self.app_id: str = ""
        self.run_id: str = ""
        self.app_cfg: Dict[str, Any] = {}
        self.msg_url: str = ""
        self.email: Optional[str] = None
        self.dag_key_value = Context._DagKeyValue(self.run_id)
        self.object_storage = Context._ObjectStorage()
        self.common = None

    def share(self, path: str, all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """copy dir (if it doesn't already exist or \"force\"=True) or file along the path starting from the \"path_prefix\" (\"apps\" directory in app by default) to the shared directory for all apps"""  # noqa: E501
        pass

    async def async_share(self, path: str, all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """copy dir (if it doesn't already exist or \"force\"=True) or file along the path starting from the \"path_prefix\" (\"apps\" directory in app by default) to the shared directory for all apps"""  # noqa: E501
        pass

    def share_many(self, paths: List[str], all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """same as share but for multiple paths, ignore not exists path"""

    async def async_share_many(self, paths: List[str], all_runs: bool = False, path_prefix: str = APP_DIR, force: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """same as share but for multiple paths, ignore not exists path"""

    def get_share_path(self, path: str, all_runs: bool = False, not_exist_ok: bool = False) -> str:  # noqa: E501
        """return real path by path, that shared before with function \"share\""""
        pass

    def delete_share(self, path: str, all_runs: bool = False, synchronize: bool = True) -> None:    # noqa: E501
        """delete dir or file, that shared between all apps, path like path is the same as used in function \"share\""""  # noqa: E501
        pass

    async def async_delete_share(self, path: str, all_runs: bool = False, synchronize: bool = True) -> None:  # noqa: E501
        """delete dir or file, that shared between all apps, path like path is the same as used in function \"share\""""  # noqa: E501
        pass

    def synchronize(self, paths: List[str] = None, all_runs: bool = False) -> None: # TODO synchronize removing  # noqa: E501
        """synchronize mounts for pods, paths = None or [] - synchronize from root mount""" # noqa: E501
        pass

    async def async_synchronize(self, paths: List[str] = None, all_runs: bool = False) -> None:  # noqa: E501
        """synchronize mounts for pods, paths = None or [] - synchronize from root mount""" # noqa: E501
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
        """only gmail work now, if subject is None used default"""
        pass

    def metadata(self, df_name: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:  # noqa: E501
        """get metadata by df_name"""
        pass

    @property
    def scale_info(self) -> Tuple[int, int]:
        pass

    def get_scale_part(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @property
    def operation_id(self) -> str:
        pass


def to_binary(smth: Any) -> bytes:  # noqa: ANN401
    return pickle.dumps(smth)


def from_binary(smth: bytes) -> Any:  # noqa: ANN401
    return pickle.loads(smth)


def load(url: str, path: str, path_prefix: str = APP_DIR) -> None:
    """path - relative: starting from the \"path_prefix\" (\"apps\" directory in app by default)"""  # noqa: E501
    pass


class S3Helper:
    def __init__(self, client: Any, s3_bucket: str) -> None:  # noqa: ANN401
        self.client = client
        self.__bucket = s3_bucket

    @staticmethod
    def create_by_cfg(cfg: Dict[str, Any], **kwargs) -> 'S3Helper':  # noqa: ANN003
        s3_client = boto3.client(
            's3',
            region_name=cfg.get('aws_region'),
            aws_access_key_id=cfg['aws_access_key_id'],
            aws_secret_access_key=cfg['aws_secret_access_key'],
            endpoint_url=cfg.get('endpoint_url'),
            **kwargs
        )
        return S3Helper(s3_client, cfg['s3_bucket'])

    def get_object(self, key: str, bucket: Optional[str]=None) -> Optional[StreamingBody]:  # noqa: E501
        pass

    def get_df(self, key: str, bucket: Optional[str]=None) -> pd.DataFrame:
        pass

    def save_object(self, body: Any, key: str, bucket: Optional[str]=None) -> None:  # noqa: ANN401
        pass

    def save_df(self, df: pd.DataFrame, key: str, bucket: Optional[str]=None) -> None:
        pass

    def delete_object(self, key: str, bucket: Optional[str]=None) -> None:
        pass


class SmtpSender:
    def __init__(self, login: str, password: str, smtp_server: str = "smtp.gmail.com", smtp_port: int = 465) -> None:  # noqa: E501
        self.server = smtp_server
        self.port = smtp_port
        self.login = login
        self.password = password

    def send(self, receivers: list[str], subject: str, message: str) -> None:
        pass


def to_df(x: Any, force: bool=False) -> pd.DataFrame:  # noqa: ANN401
    """creates a dataframe in a certain way, \"force\" should be used for complex objects ('ndarray', 'Tensor' and python primitives work without it. It crashes on basic types ('int', 'str', etc)), scheme of received dataframe - \"default_scheme\""""  # noqa: E501
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
def from_df(x: pd.DataFrame, type_name: Optional[str] = None, force: bool=False) -> Any:  # noqa: ANN401
    """decodes the \"to_df\" data from the dataframe, \"force\" is used if it was used in the encoding function - \"to_df\". You should specify the type (by type_name: for example 'ndarray', 'list', 'Tensor', 'int') that was put in this \"to_df\" dataframe.
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
