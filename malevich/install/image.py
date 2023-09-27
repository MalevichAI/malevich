"""
Provides functionality to install packages from plain Docker images
using Malevich Core capabilities
"""

import hashlib
from uuid import uuid4

import jls_utils as j

from malevich.install.installer import Installer
from malevich.install.mimic import mimic_package
from malevich.manifest import ManifestManager
from malevich.models.installers.image import ImageDependency, ImageOptions


class Templates:
    """Templates for creating a package from a Docker image"""


    imports =\
"""
from malevich._autoflow.function import autotrace
from malevich._utility.registry import Registry
from uuid import uuid4
"""
    registry =\
"""
Registry().register("{operation_id}", {{
    "image_ref": {image_ref},
    "image_auth_user": {image_auth_user},
    "image_auth_pass": {image_auth_pass},
    "processor_id": "{processor_id}",
}})
"""
    processor =\
"""
}})
"""
    processor =\
"""
@autotrace
def {name}({args}, config: dict = {{}}):
    __instance = uuid4().hex
    return (__instance, {{
        "operation_id": "{operation_id}",
        "app_cfg": config
    }})

"""
    init = \
"""
# {init_name} = ...

"""

    input = \
"""
# {input_name} = ...

"""

    output = \
"""
# {output_name} = ...

"""


class ImageInstaller(Installer):
    def __init__(
        self,
        package_name: str,
        image_ref: str,
        image_auth: tuple[str, str],
        core_host: str = "https://core.onjulius.co/",
        core_auth: tuple[str, str] = None,
    ) -> None:
        """
        Args:
            package_name: Name of the package to be created
            image_ref: Docker image reference
            image_auth: Docker image credentials
            core_host: Core host
            core_auth: Core credentials
        """
        super().__init__()
        self.__package_name = package_name
        self.__core_host = core_host
        self.__core_auth = core_auth
        self.__image_ref = image_ref
        self.__image_auth = image_auth

    @staticmethod
    def _scan_core(
        core_auth: str, core_host: str, image_ref: str, image_auth: tuple[str, str]
    ) -> j.abstract.AppFunctionsInfo:
        """Scans Core for image info

        Args:
            core_auth: Core credentials
            core_host: Core host
            image_ref: Docker image reference
            image_auth: Docker image credentials
        """
        j.set_host_port(core_host)
        if core_auth is None:
            user, pass_ = uuid4().hex, uuid4().hex
            j.create_user((user, pass_,))
        else:
            user, pass_ = core_auth
        j.update_core_credentials(user, pass_)
        app_id = uuid4().hex
        try:
            # HACK: Call create_app with empty processor_id, input_id, output_id
            # to force Core to pull image and provide info about it
            real_id = j.create_app(
                app_id=app_id,
                processor_id="",
                input_id="",
                output_id="",
                image_ref=image_ref,
                image_auth=image_auth,
            )
            # Retrieve info about app
            info = j.get_app_info(app_id, parse=True)
            # Delete app to free resources
            j.delete_app(real_id)
        except AssertionError as err_assert:
            raise Exception(
                f"Can't get info for image {image_ref}. Check that "
                "host, port, core credentials are correct and user exists. "
                f"Error: {err_assert}"
            )
        except Exception as err:
            raise Exception(
                f"Can't get info for image {image_ref}. Check that "
                "image exists and credentials are correct."
                f"Error: {err}"
            )
        else:
            return info

    @staticmethod
    def create_operations(
        operations: j.abstract.AppFunctionsInfo,
        package_name: str,
    ) -> str:
        """Generates Python code for package creation

        Args:
            operations: Operations info
            package_name: Name of the package to be created
        """
        indexed_operations = {}
        salt = hashlib.sha256(operations.model_dump_json().encode()).hexdigest()
        contents = Templates.imports
        for id_, processor in operations.processors.items():
            args_ = []

            for arg_ in processor.arguments:
                if 'return' in arg_[0] or(arg_[1] and 'Context' in arg_[1]):
                    continue
                args_.append(f'{arg_[0]}')


            args_str_ = ', '.join(args_)

            checksum = hashlib.sha256(
                (salt + processor.model_dump_json()).encode()
            ).hexdigest()

            indexed_operations[checksum] = id_

            contents += Templates.registry.format(
                operation_id=checksum,
                image_ref=('dependencies', package_name, 'options', 'image_ref'),
                image_auth_user=('dependencies', package_name, 'options', 'image_auth_user'),
                image_auth_pass=('dependencies', package_name, 'options', 'image_auth_pass'),
                processor_id=id_
            )
            contents += Templates.processor.format(
                name=id_,
                args=args_str_,
                operation_id=checksum
            )

        for id_, input_ in operations.inputs.items():
            contents += Templates.input.format(
                input_name=id_
            )

        for id_, init_ in operations.inits.items():
            contents += Templates.init.format(
                init_name=id_
            )

        for id_, output_ in operations.outputs.items():
            contents += Templates.output.format(
                output_name=id_
            )

        return contents, indexed_operations


    def install(self, *args, **kwargs) -> ImageDependency:  # noqa: ANN003, ANN002
        app_info = ImageInstaller._scan_core(
            self.__core_auth, self.__core_host, self.__image_ref, self.__image_auth
        )
        checksum = hashlib.sha256(app_info.model_dump_json().encode()).hexdigest()
        metascript, operations = ImageInstaller.create_operations(
            app_info,
            self.__package_name
        )

        mimic_package(
            self.__package_name,
            metascript,
        )

        m = ManifestManager()
        iauth_user, iauth_pass, cauth_user, cauth_token, iref = m.put_secrets(
            image_auth_user=self.__image_auth[0],
            image_auth_password=self.__image_auth[1],
            core_auth_user=self.__core_auth[0] if self.__core_auth else None,
            core_auth_token=self.__core_auth[1] if self.__core_auth else None,
            image_ref=self.__image_ref,
            salt=checksum,
        )

        return ImageDependency(
            package_id=self.__package_name,
            version='',
            installer='image',
            options=ImageOptions(
                checksum=checksum,
                core_host=self.__core_host,
                core_auth_token=cauth_token,
                core_auth_user=cauth_user,
                image_auth_user=iauth_user,
                image_auth_pass=iauth_pass,
                image_ref=iref,
                operations=operations,
            )
        )



