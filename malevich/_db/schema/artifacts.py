from sqlalchemy import Column, String, Integer, Sequence, DateTime, ForeignKey
from .base import Base


class Artifact(Base):
    __tablename__ = 'artifacts'
    
    id = Column(Integer, Sequence('artifact_seq'), primary_key=True)
    payload = Column(String)
    captured_at = Column(DateTime)
    
    session_id = Column(Integer, ForeignKey('artifacts_sessions.id'))
    
class Session(Base):
    __tablename__ = 'artifacts_sessions'
    
    id = Column(Integer, Sequence('session_seq'), primary_key=True)
    start = Column(DateTime, nullable=False)
    end = Column(DateTime)
    python_version = Column(String)
    malevich_version = Column(String)
    
    master_id = Column(Integer, ForeignKey('artifacts_master_sessions.id'))

class MasterSession(Base):
    __tablename__ = 'artifacts_master_sessions'
    
    id = Column(Integer, Sequence('master_session_seq'), primary_key=True)    

   