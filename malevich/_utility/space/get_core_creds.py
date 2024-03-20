from gql import gql
from malevich_space.ops import SpaceOps
from malevich_space.schema import SpaceSetup

from ..._db.local import db_cache_user, db_get_cached_users


def get_core_creds(setup: SpaceSetup) -> tuple[str, str]:
    org = setup.org

    if creds_ := db_get_cached_users(
        email=setup.username,
        api_url=setup.api_url,
        host=setup.host.conn_url,
        org_id=org
    ):
        return creds_

    ops = SpaceOps(setup)

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
            db_cache_user(
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
