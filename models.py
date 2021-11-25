import sys
import io
from typing import Any, List
import logging
from colorama import Style, Fore

import sqlalchemy as sql
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import func
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import DECIMAL, DateTime, Integer, String

logger = logging.getLogger(__name__)
# declarative base class
Base = declarative_base()

def get_engine(user: str, pwd: str, host: str, port: int, db: str) -> Engine:
    try:
        engine: Engine = sql.create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}", echo=False)
    except SQLAlchemyError as err:
        logging.error(err)
        raise MysqlException(err)
    return engine

def initialize_session(user: str, pwd: str, host: str, port: int, db: str) -> None:
    """Define mysql engine and set a global session"""

    global session
    try:
        engine = get_engine(user, pwd, host, port, db)
        logging.info('Got mysql client')
    except MysqlException as e:
        sys.exit(e)
    session = Session(engine)

def close_session():
    """Close global sql session"""

    session.close()
class MysqlException(Exception):
  pass

class Report:
  file_path: str
  __output__: io.StringIO

  def __init__(self, file_path, mode="w"):
    self.file_path = file_path
    self.mode = mode
    self.__output__ = self.__initialize()

  def __initialize(self) -> io.StringIO:
    return io.StringIO()

  def write(self, content: Any) -> None:
    """
    Write string content into an StringIo object.
    Every content is appended to the obejct
 
    """
    self.__output__.write(content)

  def get_content(self) -> str:
    try:
        value = self.__output__.getvalue()
    except TypeError as e:
        logger.exception(e)
        return ""
    else:
        return value

  def __close_buffer(self):
    self.__output__.close()

  def save(self) -> None:
    report: io.TextIOWrapper = open(file=self.file_path, mode=self.mode)
    content = self.get_content()
    report.write(content)
    self.__close_buffer()
    report.close()


class User(Base):
    __tablename__ = "utenti"

    id = Column('id', Integer, primary_key=True)
    name = Column('nome', String(255), nullable=True)
    first_amount = Column('primo_deposito', DECIMAL(2, 13), nullable=True)
    balance = Column('saldo', DECIMAL(2, 13), nullable=True)
    operations = relationship("Operation", back_populates="user", order_by="Operation.day", lazy="dynamic")

    @classmethod
    def get_all(cls):
        return session.query(cls).all()


class Operation(Base):
    __tablename__ = 'operazioni'

    id = Column(Integer, primary_key=True)
    user_id = Column('utente_id', ForeignKey("utenti.id"))
    day = Column('giorno', DateTime, nullable=True)
    amount = Column('ammontare', DECIMAL(2, 13), nullable=True)
    user = relationship("User", back_populates="operations")

    @classmethod
    def filter(cls, id: int) -> List:
        try:
            operations = session.query(cls.day, cls.user_id, func.sum(cls.amount).label("day_amount")) \
                    .filter_by(user_id=id) \
                    .group_by(cls.day, cls.user_id) \
                    .order_by(cls.day).all()
        except Exception as e:
            print(Fore.RED, "Error retrieving operations for user ", id, Style.RESET_ALL)
            logger.error(e)
            return []
        else:
            return operations
            
