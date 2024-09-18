from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from malevich._db.schema.base import Base
from malevich.path import Paths
from filelock import FileLock

class Read:
    def __init__(self):
        self._ref = None

    def __enter__(self):
        import sqlite3
        # HACK: Create empty db
        sqlite3.connect(Paths.db()).close()
        engine = create_engine("duckdb:///" + Paths.db())
        Base.metadata.create_all(engine)
        self._ref = Session(bind=engine)

        return self._ref
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._ref.close()
        self._ref = None
    