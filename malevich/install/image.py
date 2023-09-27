import hashlib
import os
from uuid import uuid4

import jls_utils as j

from malevich.install.installer import Installer
from malevich.models.installers.image import ImageDependency, ImageOptions


class Templates:
    imports =\
"""
from malevich._autoflow.function import autotrace
from uuid import uuid4
"""
    processor =\
"""
@autotrace
def {name}({args}):
    __instance = uuid4().hex
    return (__instance, "{operation_id}")

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
        file: str,
    ) -> None:
        salt = hashlib.sha256(operations.model_dump_json().encode()).hexdigest()
        contents = Templates.imports
        for id_, processor in operations.processors.items():
            args_ = []

            for arg_ in processor.arguments:
                if 'return' in arg_[0]:
                    continue
                if arg_[1] and 'Context' in arg_[1]:
                    args_.append('config=None')
                elif arg_[0]:
                    args_.append(f'{arg_[0]}')


            args_str_ = ', '.join(args_)

            checksum = hashlib.sha256(
                (salt + processor.model_dump_json()).encode()
            ).hexdigest()

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

        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w+') as file_:
            file_.write(contents)



    def install(self, *args, **kwargs) -> ImageDependency:  # noqa: ANN003, ANN002
        app_info = ImageInstaller._scan_core(
            self.__core_auth, self.__core_host, self.__image_ref, self.__image_auth
        )
        checksum = hashlib.sha256(app_info.model_dump_json().encode()).hexdigest()
        ImageInstaller.create_operations(
            app_info,
            os.path.join(
                ImageInstaller.get_package_path(),
                self.__package_name,
                '__init__.py'
            )
        )

        return ImageDependency(
            package_id=self.__package_name,
            version='',
            installer='image',
            options=ImageOptions(
                image_ref=self.__image_ref,
                image_auth=self.__image_auth,
                core_auth=self.__core_auth,
                core_host=self.__core_host,
                checksum=checksum
            )
        )



