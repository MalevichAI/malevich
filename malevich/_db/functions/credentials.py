from ..schema import Base, Credentials
from .get_db import get_db

def get_cached_users(
    email: str,
    api_url: str,
    host: str,
    org_id: str | None
) -> tuple[str, str] | None:
    session = get_db()
    creds: Credentials = session.query(Credentials).filter(
        Credentials.email == email,
        Credentials.host == host,
        Credentials.org_id == org_id,
        Credentials.api_url == api_url
    ).one_or_none()
    
    if creds is not None:
        return creds.core_username, creds.core_password
    else:
        return None
    

def cache_user(
    email: str,
    api_url: str,
    host: str,
    org_id: str | None,
    core_username: str,
    core_password: str
) -> None:
    session = get_db()
    session.add(
        Credentials(
            email=email,
            api_url=api_url,
            host=host,
            org_id=org_id,
            core_username=core_username,
            core_password=core_password
        )
    )
    session.commit()

