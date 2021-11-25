#!/usr/bin/python3
import argparse
import logging
import os
import pathlib
import sys
from datetime import date, datetime
from typing import List
# from time import sleep

from tqdm import tqdm

file_path = os.path.dirname(__file__)
logging.basicConfig(
    filename="./process.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

from models import Operation, Report, User, initialize_session, close_session

logger = logging.getLogger(__name__)


def process_users_operations(users: List[User], report_path: str) -> None:
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  logger.info(f'Total users: N.{len(users)}')
  for row in tqdm(users, desc="Users: ", disable=False):
    logger.info(f'Process user: {row.id} - {row.name}')
    # * Create report only if all operations succeeded
    report = Report(f'{report_path}/{row.id}.txt')
    report.write(row.name)
    report.write("\n")
    # * FEAT: aggregate operations by day and sum amount
    operations = Operation.filter(row.id)
    logger.info(f'Total operations for {row.name}: {row.operations.count()} for {len(operations)} days')
    for operation in tqdm(operations, desc=f"Processing {row.name} operations: "):
      report.write("\n")
      day: date = operation.day
      formatted_day = day.strftime('%d/%m/%Y')
      report_amount = str(round(operation.day_amount, 2)).replace('.', ',')
      report_operation = f"{formatted_day} ** {report_amount}€"
      report.write(report_operation)
      # sleep(0.1)

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

  initialize_session(user=args.user, pwd=args.password, host=args.host, port=args.port, db=args.database)

  print('Start process')
  logging.info('Start process')

  users: List[User] = User.get_all()
  today = datetime.today()
  today = today.strftime('%d-%m-%Y')
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  process_users_operations(users, report_path)
  
  close_session()
