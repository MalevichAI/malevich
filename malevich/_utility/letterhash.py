import json


def lhash_json(x: object, length=6, exp=32) -> str:
    _s = json.dumps(x, sort_keys=True)
    _i = hash(_s) % (2 ** exp)
    # Map the integer to a string with
    # characters a-z, A-Z, 0-9

    _s = ""
    alph = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for _ in range(length):
        _s += alph[_i % len(alph)]
        _i //= len(alph)
    return _s
