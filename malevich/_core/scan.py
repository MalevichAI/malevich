import malevich_coretools as core

from malevich._utility import fix_host


def scan_core(
    core_host: str,
    image_ref: str,
    image_auth: tuple[str, str],
    core_auth: tuple[str, str] | None = None,
) -> core.abstract.AppFunctionsInfo:
    """Scans Core for image info

    Args:
        core_auth: Core credentials
        core_host: Core host
        image_ref: Docker image reference
        image_auth: Docker image credentials
    """
    try:
        info = core.get_image_info(
            image_ref,
            image_auth=image_auth,
            conn_url=fix_host(core_host),
            parse=True
        )
    except Exception as err:
        raise Exception(
            f"Can't get info for image {image_ref}. Check that "
            "image exists and credentials are correct.\n"
            f"Error: {err}"
        )
    else:
        return info
