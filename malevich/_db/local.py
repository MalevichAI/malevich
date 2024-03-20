import duckdb
import os
from ..path import Paths

try:
    conn = duckdb.connect(Paths.db())
except Exception:
    os.remove(Paths.db())
    conn = duckdb.connect(Paths.db())

def get_db() -> duckdb.DuckDBPyConnection:
    conn.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS tb_creds_id;

        CREATE TABLE IF NOT EXISTS tb_creds (
            id INTEGER PRIMARY KEY DEFAULT nextval('tb_creds_id'),
            email VARCHAR(255),
            api_url VARCHAR(255),
            host VARCHAR(255),
            org_id VARCHAR(255),
            core_username VARCHAR(255),
            core_password VARCHAR(255),
        );
        """
    )
    return conn

def db_get_cached_users(
    email: str,
    api_url: str,
    host: str,
    org_id: str | None
) -> tuple[str, str] | None:
    if org_id is None:
        return get_db().sql(
            f"""SELECT core_username, core_password FROM tb_creds WHERE email = '{email}' AND api_url = '{api_url}' AND host = '{host}' AND org_id IS NULL"""
        ).fetchone()
    else:
        get_db().sql(
            f"""SELECT core_username, core_password FROM tb_creds WHERE email = '{email}' AND api_url = '{api_url}' AND host = '{host}' AND org_id = '{org_id}'
            """
        ).fetchone()

def db_cache_user(
    email: str,
    api_url: str,
    host: str,
    org_id: str | None,
    core_username: str,
    core_password: str
) -> None:
    if org_id is None:
        values_ = f"VALUES ('{email}', '{api_url}', '{host}', NULL, '{core_username}', '{core_password}')"
        where_ = f"WHERE email = '{email}' AND api_url = '{api_url}' AND host = '{host}' AND org_id IS NULL"
    else:
        values_ = f"VALUES ('{email}', '{api_url}', '{host}', '{org_id}', '{core_username}', '{core_password}')"
        where_ = f"WHERE email = '{email}' AND api_url = '{api_url}' AND host = '{host}' AND org_id = '{org_id}'"
    try:
        select_ = get_db().sql(
            f"""SELECT * FROM tb_creds {where_}"""
        ).fetchone()

        if select_:
            raise duckdb.ConstraintException

        get_db().sql(
            f"""
            INSERT INTO tb_creds (email, api_url, host, org_id, core_username, core_password)
            {values_}
            """
        )
    except duckdb.ConstraintException:

        get_db().sql(
            f"""
            UPDATE tb_creds
            SET core_username = '{core_username}', core_password = '{core_password}'
            {where_}
            """
        )

    get_db().commit()
