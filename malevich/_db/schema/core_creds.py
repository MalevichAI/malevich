from sqlalchemy import Column, String, Integer, Sequence
from .base import Base

class CoreCredentials(Base):
    __tablename__ = 'core_creds'
    
    id = Column(Integer, Sequence('core_creds_seq'), primary_key=True)
    host = Column(String(256))
    user = Column(String())
    password = Column(String())

   