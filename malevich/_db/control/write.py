from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from malevich._db.schema.base import Base
from malevich.path import Paths
from filelock import FileLock

class Write:
    def __init__(self, timeout: int | None = None):
        self._ref = None
        self._lock = FileLock(Paths.lock('.db'))
        self._tmout = timeout

    def __enter__(self):
        self._lock.acquire(timeout=self._tmout)
        try:
            import sqlite3
            # HACK: Create empty db
            sqlite3.connect(Paths.db()).close()
            engine = create_engine("sqlite:///" + Paths.db())
            Base.metadata.create_all(engine)
            self._ref = Session(bind=engine)
        except (Exception, KeyboardInterrupt) as e:
            self._lock.release()
            raise e
        
        return self._ref
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._ref.close()
        self._lock.release()
        self._ref = None
    