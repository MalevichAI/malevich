import dill

def capture_function(__function) -> bytes:
    """Capture a function for later use."""
    return dill.dumps(__function, recurse=True)