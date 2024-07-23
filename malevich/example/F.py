
import typing
from typing import *

import malevich.annotations
from malevich.models import ConfigArgument
from malevich._meta.decor import proc
from malevich._utility import Registry
from malevich.models.nodes import OperationNode
from .scheme import *

Registry().register("9d0df64ebb3e363b64c401d2443b2f5fbf8915c3238fb8cdc63ed24ffcd984d5", {'operation_id': '9d0df64ebb3e363b64c401d2443b2f5fbf8915c3238fb8cdc63ed24ffcd984d5', 'image_ref': ('dependencies', 'example', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'example', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'example', 'options', 'image_auth_pass'), 'processor_id': 'simple_document_processor'})

@proc(use_sinktrace=False, config_model=None)
def simple_document_processor(
    request: malevich.annotations.OpResult | malevich.annotations.Collection,
    /, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """None"""

    return OperationNode(
        operation_id="9d0df64ebb3e363b64c401d2443b2f5fbf8915c3238fb8cdc63ed24ffcd984d5",
        config=config,
        processor_id="simple_document_processor",
        package_id="example",
        alias=alias,
    )

Registry().register("774a70a2fd82e82587ee4cdae78830460a39af3766daf52187411817f1d2ecd5", {'operation_id': '774a70a2fd82e82587ee4cdae78830460a39af3766daf52187411817f1d2ecd5', 'image_ref': ('dependencies', 'example', 'options', 'image_ref'), 'image_auth_user': ('dependencies', 'example', 'options', 'image_auth_user'), 'image_auth_pass': ('dependencies', 'example', 'options', 'image_auth_pass'), 'processor_id': 'complex_sink'})

@proc(use_sinktrace=True, config_model=None)
def complex_sink(
    left: malevich.annotations.OpResult | malevich.annotations.Collection,
    right: malevich.annotations.OpResult | malevich.annotations.Collection,
    /,
    *sink: Any, 
    alias: Optional["str"] = None,
    config: Optional[dict] = None, 
    **extra_config_fields: dict[str, Any]) -> malevich.annotations.OpResult:
    """None"""

    return OperationNode(
        operation_id="774a70a2fd82e82587ee4cdae78830460a39af3766daf52187411817f1d2ecd5",
        config=config,
        processor_id="complex_sink",
        package_id="example",
        alias=alias,
    )
