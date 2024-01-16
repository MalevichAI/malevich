import importlib
from enum import Enum

import pandas as pd
from malevich._utility.package import package_manager as pm
from malevich.models.flow_function import FlowFunction
from malevich.models.manifest import Dependency
from malevich.cli import auto_use, remove
from malevich import manifest
from malevich._meta.flow import R as _Meta_Flow_R
from malevich.models.results.base import BaseResult
from ..fixtures.core_provider import CoreProvider
from ..fixtures.space_provider import SpaceProvider
from typing import ParamSpec, TypeVar
from malevich._utility.registry import Registry

class TestingScope(Enum):
    CORE = "CORE"
    SPACE = "SPACE"

    def installer(self) -> str:
        if self == TestingScope.CORE:
            return 'image'
        elif self == TestingScope.SPACE:
            return 'space'
        else:
            raise NotImplementedError
        
    def prepare_installer(self) -> None:
        if self == TestingScope.CORE:
            return
        elif self == TestingScope.SPACE:
            from ..fixtures.space_provider import SpaceProvider
            SpaceProvider().assert_manifest()
        else:
            raise NotImplementedError


class FlowTestRunner:
    _Params = ParamSpec('_Params')
    _Return = _Meta_Flow_R
    def __init__(self, scope) -> None:
        if scope not in TestingScope:
            raise ValueError(f"Scope {scope} is not supported")

        if scope == TestingScope.CORE:
            self.provider = CoreProvider()
        elif scope == TestingScope.SPACE:
            self.provider = SpaceProvider()

        self.scope = scope
            
    def full_test(self, flow: FlowFunction[_Params, _Meta_Flow_R], *args, **kwargs) -> list[BaseResult]:
        task = flow(*args, **kwargs)
        task.interpret(self.provider.get_interpreter())
        if self.scope == TestingScope.SPACE:    
            task.prepare()
            task.run()
        else:
            task.prepare(with_logs=True, debug_mode=True)
            task.run(with_logs=True, debug_mode=True, profile_mode='all')
        task.stop()
        return task.results()
    
    def interpret(self, flow: FlowFunction[_Params, _Meta_Flow_R], *args, **kwargs) -> None:
        task = flow(*args, **kwargs)
        task.interpret(self.provider.get_interpreter())
        return task
    
    def prepare(self, task: _Meta_Flow_R, *args, **kwargs) -> None:
        task.prepare(*args, **kwargs)
        return task
    
    def run(self, task: _Meta_Flow_R, *args, **kwargs) -> None:
        task.run(*args, **kwargs)
        return task
    
    def stop(self, task: _Meta_Flow_R, *args, **kwargs) -> None:
        task.stop(*args, **kwargs)
        return task
    
    def results(self, task: _Meta_Flow_R, *args, **kwargs) -> None:
        return task.results(*args, **kwargs)
    


class FlowTestEnv:
    def __init__(
        self,
        dependencies: list[str],
        scope: TestingScope,
        install_args: list[dict[str, str]] or dict[str, str] = {},
    ):
        self.dependencies = dependencies
        if isinstance(install_args, list):
            self.__indepenent_args = True
            self.__with_args = [
                ','.join([
                    f'{key}={v}'
                    for key, v in arg_.items()
                ]) for arg_ in install_args
            ]
        else:
            self.__indepenent_args = False
            self.__with_args = ','.join([
                f'{key}={v}'
                for key, v in install_args.items()
            ])

        self.scope = scope

    def is_env_ready(self) -> bool:
        ready_ = True
        for dependency in self.dependencies:
            if not pm.is_importable(dependency):
                return False

            options = manifest.query(
                'dependencies',
                dependency,
            )
            if not options:
                return False
            
            options = Dependency(**options)
            ready_ &= options.installer == self.scope.installer()
            ready_ &= options.package_id == dependency

        return ready_

    def install_dependencies(self) -> None:        
        remove(self.dependencies)
        Registry().registry.clear()
        self.scope.prepare_installer()
        if self.__indepenent_args:
            for dependency, args in zip(self.dependencies, self.__with_args):
                auto_use([dependency], self.scope.installer(), with_args=args)
        else:
            auto_use(self.dependencies, self.scope.installer(), with_args=self.__with_args)
        for dependency in self.dependencies:            
            module = importlib.import_module(f'malevich.{dependency}')
            importlib.reload(module) 


    def remove_dependencies(self) -> None:
        for dependency in self.dependencies:
            try: 
                pm.remove_stub(dependency)
                manifest.remove('dependencies', dependency)
            except Exception:
                pass

    def __enter__(self) -> FlowTestRunner:
        if not self.is_env_ready():
            self.remove_dependencies()
            self.install_dependencies()

        return FlowTestRunner(self.scope)

    def __exit__(self, *args):
        self.remove_dependencies()
