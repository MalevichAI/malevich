from pydantic import BaseModel, ConfigDict


class _Model(BaseModel):
    model_config = ConfigDict(
        protected_namespaces=(),
    )
