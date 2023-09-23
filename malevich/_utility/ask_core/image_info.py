from uuid import uuid4

from jls_utils import (
    create_app,
    delete_app,
    get_app_info,
    set_host_port,
    update_core_credentials,
)

from malevich.models.core_app_info import AppFunctionsInfo


def get_image_info(
    image_ref: str,
    image_auth: tuple[str, str],
    core_auth: tuple[str, str] = None,
    host: str = None,
) -> AppFunctionsInfo:
    """Get info about image using Core API

    Args:
        image_ref (str): image reference
        image_auth (tuple[str, str]): image credentials
        core_auth (tuple[str, str], optional): core credentials. Defaults to None.
        host (str, optional): core host. Defaults to None.

    Returns:
        AppFunctionsInfo: info about image

    Raises:
        Exception: if can't get info about image
    """
    # If host is not None, set it
    # otherwise, assume that host is already set
    if host:
        set_host_port(host)

    # If core_auth is not None, set it
    # otherwise, assume that core_auth is already set
    if core_auth:
        update_core_credentials(*core_auth)

    # HACK: Create app with random id and get info about it
    app_id = f"__get_info_{uuid4()}"

    try:
        # HACK: Call create_app with empty processor_id, input_id, output_id
        # to force Core to pull image and provide info about it
        real_id = create_app(
            app_id=app_id,
            processor_id="",
            input_id="",
            output_id="",
            image_ref=image_ref,
            image_auth=image_auth,
        )
        # Retrieve info about app
        info = get_app_info(app_id)
        # Delete app to free resources
        delete_app(real_id)
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
        # parsed_info = AppFunctionsInfo.model_validate(info)
        return info
