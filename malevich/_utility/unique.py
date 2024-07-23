from collections import Counter

__unique = Counter()

def unique(prefix: str):
    __unique.update([prefix])
    return f"{prefix}_{__unique[prefix]}"
