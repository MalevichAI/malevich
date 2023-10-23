def fix_host(host: str) -> str:
    if not host:
        return host
    if not host.endswith('/'):
        return host + '/'
    return host
