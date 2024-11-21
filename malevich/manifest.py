import os
import re
from copy import deepcopy
from typing import Any, Iterable, Optional

import pydantic_yaml as pydml

from malevich._dev.singleton import SingletonMeta
from malevich.models.manifest import Manifest, Secret, Secrets
from malevich.path import Paths


class OverrideManifest:
    def __init__(self, path: str) -> None:
        self._path = path
        self._backup_path = getattr(ManifestManager(), "_ManifestManager__path")
        self._backup_secrets_path = getattr(
            ManifestManager(), "_ManifestManager__secrets_path"
        )

    def __enter__(self) -> None:
        self._instance = ManifestManager(self._path)

        setattr(
            self._instance,
            "_ManifestManager__path",
            os.path.join(self._path, "malevich.yaml")
        )

        setattr(
            self._instance,
            "_ManifestManager__secrets_path",
            os.path.join(self._path, "malevich.secrets.yaml")
        )

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        setattr(ManifestManager(), "_ManifestManager__path", self._backup_path)
        setattr(ManifestManager(), "_ManifestManager__secrets_path", self._backup_secrets_path)  # noqa: E501


class ManifestManager(metaclass=SingletonMeta):
    @staticmethod
    def secret_pattern() -> str:
        return r"secret#[0-9]{1,6}"

    @staticmethod
    def is_secret(value: str) -> bool:
        return re.match(ManifestManager.secret_pattern(), str(value)) is not None

    @staticmethod
    def create_manifest() -> None:
        pydml.to_yaml_file(Paths.pwd('malevich.yaml'), Manifest())
        pydml.to_yaml_file(Paths.pwd('malevich.secrets.yaml'), Secrets())

    @property
    def path(self) -> str:
        return '' + self.__path

    def __init__(self, workdir: str | None = None) -> None:
        if os.path.exists(Paths.pwd('malevich.yaml')):
            self.__path = Paths.pwd('malevich.yaml')
            self.__secrets_path = Paths.pwd('malevich.secrets.yaml', create=True)
            self.local = True
        else:
            self.__path = Paths.home('malevich.yaml', create=True)
            self.__secrets_path = Paths.home('malevich.secrets.yaml', create=True)
            self.local = False

        if workdir is None:
            workdir = os.getcwd()

        with open(self.__path, 'r+') as _file:
            if os.path.getsize(self.__path) == 0:
                _file.write('{}')
                _file.seek(0)
            self.__manifest = pydml.parse_yaml_file_as(Manifest, _file)
            self.__backup = self.__manifest.model_dump()

        with open(self.__secrets_path, 'r+') as _file:
            if os.path.getsize(self.__secrets_path) == 0:
                _file.write('{}')
                _file.seek(0)

            self.__secrets = pydml.parse_yaml_file_as(
                Secrets, _file
            )

    def cleanup_secrets(self) -> list[Secret]:
        secrets = re.findall(
            r"secret#[0-9]{1,6}", self.__manifest.model_dump_json(indent=4)
        )
        cleaned_secrets = Secrets(
            secrets={
                key: value
                for key, value in self.__secrets.model_dump()["secrets"].items()
                if key in secrets
            }
        )

        return cleaned_secrets

    def save(self) -> None:
        try:
            pydml.to_yaml_file(self.__path, self.__manifest)
            self.__backup = self.__manifest.model_dump()
        except Exception as e:
            pydml.to_yaml_file(self.__path, Manifest(**self.__backup))
            raise e
        finally:
            self.__manifest = pydml.parse_yaml_file_as(Manifest, self.__path)

        # self.__secrets = self.cleanup_secrets()
        pydml.to_yaml_file(self.__secrets_path, self.__secrets)
        self.__secrets = pydml.parse_yaml_file_as(Secrets, self.__secrets_path)

    @staticmethod
    def __map(obj, f) -> dict | list | None:
        """Deep key map"""
        if isinstance(obj, list):
            for i, x in enumerate(obj):
                obj[i] = ManifestManager.__map(x, f)
        if isinstance(obj, dict):
            for x in obj:
                obj[x] = ManifestManager.__map(obj[x], f)
        else:
            obj = f(obj)
        return obj

    def __query_list(
            self, __list: list[str | dict[str, Any]], __key: str
        ) -> Any:  # noqa: ANN401
        # Iterate over list object
        # within the Manifest
        # If list item is a dict, we need to check
        # if the key is in the dict

        # If there are multiple dicts with the same key,
        # we raise an error
        found = False
        for value in filter(lambda x: isinstance(x, dict), __list):
            if __key in value:
                if found:
                    raise ValueError(
                        f"Multiple values found for key {__key} in Manifest"
                    )
                found = True

        # If there are multiple strings with the same value,
        # we raise an error
        for value in filter(lambda x: isinstance(x, str), __list):
            if value == __key:
                if found:
                    raise ValueError(
                        f"Multiple values found for key {__key} in Manifest"
                    )
                found = True

        # After checking for multiple values,
        # we can iterate over the list again
        # and return the first value that matches
        for value in __list:
            if isinstance(value, dict) and __key in value:
                return value[__key]
            elif isinstance(value, str) and value == __key:
                return value
            elif isinstance(value, list):
                raise ValueError("Nested lists are not supported in Manifest")
        return None

    def query(
        self, *query: Iterable[str], resolve_secrets: bool = False
    ) -> Any:  # noqa: ANN401
        # Only iterate over dict representation of Manifest
        # for simplicity
        cursor = self.__manifest.model_dump()
        for key in query:
            # If the cursor is a list, we need to iterate over it
            if isinstance(cursor, list):
                cursor = self.__query_list(cursor, key)
            elif isinstance(cursor, dict):
                cursor = cursor[key]
            else:
                return None

        if resolve_secrets:
            def __mapf(x) -> str:
                if (
                    x is not None
                    and isinstance(x, str)
                    and re.match(r"^secret#[0-9]{1,6}", x)
                ):
                    return self.query_secret(x, only_value=True)
                else:
                    return x

            return ManifestManager.__map(cursor, __mapf)
        return cursor
        # if (
        #     cursor is not None
        #     and resolve_secrets
        #     and isinstance(cursor, str)
        #     and re.match(r"^secret#[0-9]{1,6}", cursor)
        # ):
        #     return self.query_secret(cursor).secret_value
        # return cursor

    def put(
        self, *path: Iterable[str], value: Any, append: bool = False  # noqa: ANN401
    ) -> Manifest:
        pre = path[:-1]
        key = path[-1]
        dump = self.__manifest.model_dump()
        cursor = dump
        for q in pre:
            if isinstance(cursor, list):
                try:
                    q = int(q)
                    __cursor = cursor[q]
                except ValueError:
                    pass
                else:
                    cursor = __cursor
                    continue

                for i, _c in enumerate(cursor):
                    if q in _c:
                        cursor = cursor[i][q]
                        break
                else:
                    cursor.append({})
                    cursor = cursor[-1]
                    continue
            if q not in cursor:
                cursor[q] = {}
            cursor = cursor[q]
        if isinstance(cursor, list):
            if append:
                cursor.append(value)
            else:
                for i, _c in enumerate(cursor):
                    if key in _c:
                        cursor[i][key] = value
                        break
        else:
            if append:
                cursor[key].append(value)
            else:
                cursor[key] = value
        self.__manifest = Manifest(**dump)
        self.save()
        return self.__manifest

    def put_secret(
        self,
        key: str,
        value: str,
        salt: Optional[str] = None,
        external: bool = False
    ) -> str:
        # Create new secret and hash it with 6 digits
        if salt:
            secret_hash_id = hash(f'{key}="{value}"{salt}'.encode()) % 1000000
        else:
            secret_hash_id = hash(f'{key}="{value}"'.encode()) % 1000000
        __key = f"secret#{secret_hash_id}"
        secrets = self.__secrets.model_dump()
        secrets["secrets"][__key] = Secret(
            secret_key=key, secret_value=value, salt=salt, external=external
        )
        self.__secrets = Secrets(**secrets)
        return __key

    def put_secrets(
        self, salt: Optional[str] = None, **kwargs: dict[str, str]
    ) -> list[str]:
        secrets = []
        for key, value in kwargs.items():
            if value:
                secrets.append(self.put_secret(key, value, salt))
            else:
                secrets.append(None)
        return secrets

    def query_secret(self, key: str, only_value: bool = False) -> Secret | None:
        _s = self.__secrets.secrets.get(key, None)
        if _s and only_value:
            return _s.secret_value
        return _s

    def as_dict(self) -> dict[str,]:
        return deepcopy(self.__manifest.model_dump())

    def get_secrets(self) -> Secrets:
        return deepcopy(self.__secrets.model_dump())

    def remove(self, *path: Iterable[str]) -> Manifest:
        pre = path[:-1]
        key = path[-1]
        dump = self.__manifest.model_dump()
        cursor = dump
        for q in pre:
            if isinstance(cursor, list):
                try:
                    q = int(q)
                    __cursor = cursor[q]
                except ValueError:
                    pass
                else:
                    cursor = __cursor
                    continue

                for i, _c in enumerate(cursor):
                    if q in _c:
                        cursor = cursor[i][q]
                        break
                else:
                    cursor.append({})
                    cursor = cursor[-1]
                    continue
            if q not in cursor:
                cursor[q] = {}
            cursor = cursor[q]
        if isinstance(cursor, list):
            for i, _c in enumerate(cursor):
                if key in _c:
                    cursor.pop(i)
                    break
        else:
            cursor.pop(key)
        self.__manifest = Manifest(**dump)
        self.save()
        return self.__manifest

try:
    manf = ManifestManager()
except FileNotFoundError:
    manf = None
