import sys
from datetime import datetime

from malevich._db import Write

from .._db.schema.artifacts import MasterSession, Session
from .._utility.malevich_version import get_malevich_version


def decide_session() -> Session:
    with Write() as db_ref:
        sessions = db_ref.query(Session).all()

        # Search for session less than an hour ago
        current_time = datetime.now()
        sessions = [
            session for session in sessions if session.end is not None
        ]

        sessions.sort(key=lambda x: x.end, reverse=True)
        r_session = None
        for session in sessions:
            if session.end and (current_time - session.end).seconds < 3600:
                r_session = session
                break

        if r_session is None:
            m_session = MasterSession()
            db_ref.add(m_session)
            db_ref.commit()
            r_session = Session(
                start=current_time,
                python_version='.'.join(map(str, sys.version_info[:3])),
                malevich_version=get_malevich_version(),
                master_id=m_session.id
            )
            db_ref.add(r_session)
            db_ref.commit()

        return r_session.id
