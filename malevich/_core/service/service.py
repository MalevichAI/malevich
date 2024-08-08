import malevich_coretools as api  # noqa: I001

from .base import BaseCoreService
from .asset import AssetService
from .collection import CollectionService
from .configuration import ConfigService
from .pipeline import PipelineService
from .document import DocumentService
from .runs import RunService

class CoreService(BaseCoreService):
    def __init__(self, auth: api.AUTH, conn_url: str) -> None:
        super().__init__(auth, conn_url)

        self.collection = CollectionService(auth, conn_url)
        self.cfg = ConfigService(auth, conn_url)
        self.asset = AssetService(auth, conn_url)
        self.pipeline = PipelineService(auth, conn_url)
        self.document = DocumentService(auth, conn_url)
        self.run = RunService(auth, conn_url)
        self.api = api
