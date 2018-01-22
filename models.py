from sqlalchemy import create_engine, Column, BigInteger, Date, String
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

Base = declarative_base()


class PlayerInfo(Base):
    __tablename__ = 'user_detail'
    id = Column(BigInteger, primary_key=True)
    uuid = Column(BigInteger, unique=True)
    userDesc = Column(String(255))
    createAt = Column(Date, default=None)
    updateAt = Column(Date, default=None)
    suspensionExpiredDate = Column(Date, default=None)


def init_db():
    engine = create_engine('mysql+pymysql://root:1234@localhost:3306/test',poolclass=NullPool)
    DBSession = scoped_session(sessionmaker(bind=engine))
    return DBSession
