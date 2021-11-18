#!/usr/bin/python3
import argparse
import logging
import os
import pathlib
import sys
from datetime import datetime
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import select
from tqdm import tqdm
import sqlalchemy as sql
from models import MysqlException, Report, User

file_path = os.path.dirname(__file__)
logging.basicConfig(
    filename="./process.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

def get_engine(user: str, pwd: str, host: str, port: int, db: str) -> Engine:
  try:
    engine: Engine = sql.create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}", echo=False)
  except SQLAlchemyError as err:
    logging.error(err)
    raise MysqlException(err)
  
  return engine


def process_users_operations(data, report_path: str) -> None:
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  for row in tqdm(data.scalars().all()):
    print(row.id, ' - ', row.name)
    # * Create report only if all operations succeeded
    report = Report(f'{report_path}/{row.id}.txt')
    for operation in row.operations.scalars().all():
      print('operation')
    # report.write(row[1])
    # report.write("\n")
    report.close()

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
  users = session.execute(select_users)
  today = datetime.today()
  today = today.strftime('%d-%m-%Y')
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  process_users_operations(data=users, report_path=report_path)

