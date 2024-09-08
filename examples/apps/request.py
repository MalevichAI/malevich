from malevich.square import processor, scheme, init, DF, DFS, Context, Doc
from typing import Any, Optional, Literal

@init(prepare=True)
def create_session_manager(ctx: Context):
    manager= ... # Create manager
    ctx.common = manager # Assign manager to common

@scheme()
class Endpoint:
  """A scheme that defines endpoints to send requests to"""
  url: str  # URL of the endpoint
  headers: dict[str, str]  # Headers to set

@scheme()
class Request:
  """A scheme that defines requests itself"""
  verb: Literal['GET', 'PATCH', 'POST', 'DELETE']  # HTTP request verb
  body: Optional[dict[Any, Any] | list[Any]] = None  # Body for non-GET requests
  params: Optional[dict[str, Any]]  # Query params

@scheme()
class Metadata: 
  """Extra data for requests"""
  timestamp: int  # Request init time

@processor()
def send_user_requests(
  requests: DFS[Doc[Endpoint], Doc[Request]],
  metadata: Doc[Metadata],
  context: Context
) -> Doc:
  """The processor sends requests with HTTP protocol

  It can be connected to two processors as it requires two arguments. The first
  connected processor should return two documents of schemas Endpoint and Request
  respectively:

  ```python
  def prepare_request(...) -> tuple[Doc, Doc]:
      # Example of such processor
      return {'url': ...}, {'verb': 'GET', 'params': { ... }, ... } 
  ```
  The second connected processor should return a single document 
  with schema Metadata:
 
 ```python
  def prepare_metadata(...) -> Doc:
      # Example of such processor
      return {'timestamp': int(datetime.now().timestamp())}
  ```
  
  The processor uses common for all runs request manager defined in context.common for sending requests.
  
  The processor also uses a configuration to set extra session headers. 
  The configuration is arbitrary, so it simply takes `context.app_cfg`
  and pops internal `__core__` field to not confuse it with header.

  The processor returns a JSON document as a response, so its return 
  type is Doc and the return statement contains a dict 

  """

  endpoint = requests[0].parse()
  request = requests[0].parse()
  meta = metadata.parse()
  session_headers = context.app_cfg
 
  # Pop internal field
  context.app_cfg.pop('__core__')
  
  response = context.common.send_request() # Send logic
  
  return response.json()