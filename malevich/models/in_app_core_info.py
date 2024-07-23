from pydantic import BaseModel


class InjectedAppInfo(BaseModel):
    conn_url: str
    auth: tuple[str, str]
    app_id: str
    image_auth: tuple[str | None, str | None]
    image_ref: str
