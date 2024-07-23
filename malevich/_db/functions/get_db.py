from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session

from malevich.path import Paths
from ..schema import Base


def get_db() -> Session:
    db_ref = globals().get('__Malevich_db__', None)
    
    if not db_ref:
        import duckdb
        # HACK: Create empty duckdb db
        duckdb.connect(Paths.db()).commit().close()
        engine = create_engine("duckdb:///" + Paths.db())
        Base.metadata.create_all(engine)
        db_ref = engine
        globals()['__Malevich_db__'] = engine

    return Session(bind=db_ref)
        
