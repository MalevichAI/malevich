def parse_kv_args(raw_string: str) -> dict[str, str]:
    i = 0
    n = len(raw_string)
    in_quotes = False
    escaped = False
    buffer = ""
    key = ""
    kvargs = {}
    while i < n:
        if raw_string[i] == '"' and not escaped:
            in_quotes = not in_quotes
        elif raw_string[i] == '\\' and not escaped:
            escaped = True
        elif raw_string[i] in ' \t' and not in_quotes:
            i += 1
            continue
        elif raw_string[i] == '=' and not in_quotes:
            if buffer:
                key = buffer
                buffer = ""
            else:
                raise ValueError("Argument key cannot be empty")
        elif raw_string[i] in ', \t' and not in_quotes:
            if buffer:
                try:
                    kvargs[key.strip()] = buffer
                except ValueError:
                    raise ValueError(f"Invalid key {key.strip()}")
                key = ""
                buffer = ""
        else:
            buffer += raw_string[i]
        i += 1

    if buffer:
        kvargs[key] = buffer

    if in_quotes:
        raise ValueError("Unmatched quotes")

    return kvargs
