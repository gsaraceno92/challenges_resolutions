#!/usr/bin/python3
import argparse
import logging
import os
import pathlib
import sys
from datetime import date, datetime


import sqlalchemy as sql
from sqlalchemy.engine import ResultProxy
from sqlalchemy import select
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tqdm import tqdm

file_path = os.path.dirname(__file__)
logging.basicConfig(
    filename="./process.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

from models import MysqlException, Report, User

logger = logging.getLogger(__name__)

def get_engine(user: str, pwd: str, host: str, port: int, db: str) -> Engine:
  try:
    engine: Engine = sql.create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}", echo=False)
  except SQLAlchemyError as err:
    logging.error(err)
    raise MysqlException(err)
  
  return engine


def process_users_operations(data: ResultProxy, report_path: str) -> None:
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  users = data.scalars().all()
  logger.info(f'Total users: N.{len(users)}')
  # TODO: use tdqm_notebook for a better user experience
  for row in tqdm(users, desc="Users progress: "):
    logger.info(f'Process user: {row.id} - {row.name}')
    # * Create report only if all operations succeeded
    report = Report(f'{report_path}/{row.id}.txt')
    report.write(row.name)
    report.write("\n")
    logger.info(f'Total operations for {row.name}: N.{len(row.operations)}')
    # TODO: sort operations for asc date
    for operation in tqdm(row.operations, desc="Operations progress: "):
      report.write("\n")
      day: date = operation.day
      formatted_day = day.strftime('%d/%m/%Y')
      report_amount = str(round(operation.amount, 2)).replace('.', ',')
      report_operation = f"{formatted_day} ** {report_amount}€"
      report.write(report_operation)

    report.save()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Args to be processed')
  parser.add_argument('--host',
                      nargs='?',
                      const='localhost',
                      default='localhost',
                      type=str,
                      help="mysql database endpoint")
  parser.add_argument('-d', '--database',
                      type=str,
                      default='batch',
                      help="mysql database")
  parser.add_argument('--port',
                      nargs='?',
                      const=3306,
                      default=3306,
                      type=int,
                      help="mysql database endpoint")
  parser.add_argument(
      '-pwd',
      '--password',
      nargs='?',
      const='resu',
      default='resu',
      type=str,
      dest='password',
      help='Mysql database password')
  parser.add_argument(
      '-u',
      '--user',
      type=str,
      nargs='?',
      const='user',
      default='user',
      dest='user',
      help='Mysql database user'
  )

  args = parser.parse_args()
  logging.debug('Get script args')

  try:
    engine = get_engine(user=args.user, pwd=args.password, host=args.host, port=args.port, db=args.database)
    logging.info('Got mysql client')
  except MysqlException as e:
    sys.exit(e)

  print('Start process')
  logging.info('Start process')
  session = Session(engine)

  select_users = select(User)
  users: ResultProxy = session.execute(select_users)
  today = datetime.today()
  today = today.strftime('%d-%m-%Y')
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  process_users_operations(data=users, report_path=report_path)

