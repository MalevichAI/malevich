import pandas as pd
from malevich.square import processor, scheme, Context, Doc
from pydantic import BaseModel, Field
from typing import Optional

@scheme()
class PackageData(BaseModel):
    """Model for package metadata

    Fields:
        package (str): name of the package
        method (str): needed method
    """
    package: str
    method: str
    
@scheme()
class TransformedMethod(BaseModel):
    """Model for describing recreated methods

    Fields:
        imports (list[str]): list of packages to import
        signature (str): signature of the function
        args (dict[str, str]): dictionary of argument names and their types
        output_type (str): output type
        body (str): body of the function in plain text
    """
    imports: list[str]
    decorator: str
    signature: str
    args: dict[str, str] 
    output_type: str
    body: str
    
@processor()
def generate_method(data: Doc[PackageData], context: Context) -> Doc[TransformedMethod]:
    """Processor for recreating a method from a package present in pip into a Malevich processor.
    
    Args:
        data (Doc[PackageData]): a JSON object that contains package name and name of the method
        context (Context): context. Not used in the processor

    Returns:
        Doc[Transformed Method]: a JSON object with a processor metadata, containing a list of packages to import, decorator, function signature, dictionary of argument types that can be formatted into the function, output type and a function body itself.
    """
    ... # Some calculations
    return {
        'imports': ['asyncio', 'aiohttp'],
        'decorator': '@processor',
        'signature': 'async def post(headers: {headers}, data: {data}, ctx: Context) -> {output_type}', 
        'args': {
            'headers': 'Doc',
            'data': 'Doc'
        },
        'output_type': 'Doc',
        'body': '...'
    }