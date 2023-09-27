class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):  # noqa: ANN003, ANN002, ANN204
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
