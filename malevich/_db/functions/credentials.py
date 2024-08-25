from malevich._db import Read, Write
from ..schema import Base, CachedCredentials

def get_cached_users(
    email: str,
    api_url: str,
    host: str,
    org_id: str | None
) -> tuple[str, str] | None:
    with Read() as session:
        creds: CachedCredentials = session.query(CachedCredentials).filter(
            CachedCredentials.email == email,
            CachedCredentials.host == host,
            CachedCredentials.org_id == org_id,
            CachedCredentials.api_url == api_url
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
    with Read() as session:
        creds: CachedCredentials = session.query(CachedCredentials).filter(
            CachedCredentials.email == email,
            CachedCredentials.host == host,
            CachedCredentials.org_id == org_id,
            CachedCredentials.api_url == api_url
        ).one_or_none()
    
    with Write() as session:
        if not creds:
            session.add(
                CachedCredentials(
                    email=email,
                    api_url=api_url,
                    host=host,
                    org_id=org_id,
                    core_username=core_username,
                    core_password=core_password
                )
            )
        else:
            creds.core_username = core_username
            creds.core_password = core_password
        session.commit()

