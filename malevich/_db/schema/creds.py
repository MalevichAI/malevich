from sqlalchemy import Column, Integer, Sequence, String, create_engine
from .base import Base

class CachedCredentials(Base):
    __tablename__ = 'tb_creds'
    
    """
        CREATE SEQUENCE IF NOT EXISTS tb_creds_id;

        CREATE TABLE IF NOT EXISTS tb_creds (
            id INTEGER PRIMARY KEY DEFAULT nextval('tb_creds_id'),
            email VARCHAR(255),
            api_url VARCHAR(255),
            host VARCHAR(255),
            org_id VARCHAR(255),
            core_username VARCHAR(255),
            core_password VARCHAR(255),
        );
    """
    
    id = Column(Integer, Sequence('tb_creds_id'), primary_key=True)
    email = Column(String(255))
    api_url = Column(String(255))
    host = Column(String(255))
    org_id = Column(String(255), nullable=True)
    core_username = Column(String(255))
    core_password = Column(String(255))
    
