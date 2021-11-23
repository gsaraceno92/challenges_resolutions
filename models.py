
import io
from typing import Any
import logging

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import DECIMAL, DateTime, Integer, String

logger = logging.getLogger(__name__)
# declarative base class
Base = declarative_base()
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
  operations = relationship("Operation", back_populates="user")


class Operation(Base):
    __tablename__ = 'operazioni'

    id = Column(Integer, primary_key=True)
    user_id = Column('utente_id', ForeignKey("utenti.id"))
    day = Column('giorno', DateTime, nullable=True)
    amount = Column('ammontare', DECIMAL(2, 13), nullable=True)
    user = relationship("User", back_populates="operations")
