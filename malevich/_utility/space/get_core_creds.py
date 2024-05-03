from gql import gql
from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup

from malevich.core_api import check_auth

from ..._db import cache_user, get_cached_users, get_db
from ..._db.schema.core_creds import CoreCredentials


def get_core_creds_from_setup(setup: SpaceSetup) -> tuple[str, str]:
    org = setup.org

    if creds_ := get_cached_users(
        email=setup.username,
        api_url=setup.api_url,
        host=setup.host.conn_url,
        org_id=org
    ):
        try:
            check_auth(auth=creds_, conn_url=setup.host.conn_url)
        except Exception:
            pass
        else:
            return creds_

    ops = SpaceOps(setup)
    if org is not None:
        org = ops.get_org(reverse_id=org).uid

    r = ops.client.execute(
        gql("""
        query GetAllHosts {
            hosts {
                all {
                    edges {
                        node {
                            details {
                                uid
                                connUrl
                            }
                        }
                    }
                }
            }
        }
        """)
    )

    hosts = [
        (
            x['node']['details']['connUrl'],
            x['node']['details']['uid']
        )
        for x in r['hosts']['all']['edges']
    ]

    uid = None
    for conn_url, uid_ in hosts:
        if conn_url == setup.host.conn_url:
            uid = uid_
            break
    else:
        raise Exception("Host not found")

    r = ops.client.execute(
        gql("""
            query GetAllSAs($host_id: String!) {
                host(uid: $host_id) {
                    mySaOnHost {
                        edges {
                            node {
                                details {
                                    corePassword
                                    coreUsername
                                }
                            }
                        }
                    }
                }
            }
        """), variable_values={'host_id': uid}
    )

    sas = [
        (
            x['node']['details']['coreUsername'],
            x['node']['details']['corePassword']
        )
        for x in r['host']['mySaOnHost']['edges']
    ]

    for u, p in sas:
        parts_ = u.split('__')
        if (
            len(parts_) == 2 and parts_[0] == org
            or len(parts_) == 1 and org is None
        ):
            try:
                check_auth(auth=(u, p,), conn_url=setup.host.conn_url)
            except Exception:
                pass
            cache_user(
                email=setup.username,
                api_url=setup.api_url,
                host=setup.host.conn_url,
                org_id=org,
                core_username=u,
                core_password=p
            )
            return u, p
    else:
        raise Exception("SA not found")

def get_core_creds_from_db(user: str, host: str):
    creds_ = get_db().query(CoreCredentials).where(
        CoreCredentials.user == user,
        CoreCredentials.host == host
    ).one_or_none()

    if not creds_:
        return None

    return creds_.user, creds_.password
