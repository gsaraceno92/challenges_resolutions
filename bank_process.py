#!/usr/bin/python3
import argparse
import logging
import os
import pathlib
import sys
from datetime import datetime
from io import TextIOWrapper

import mysql.connector
from mysql.connector import errorcode
from mysql.connector.connection import MySQLConnection
from tqdm import tqdm

from models import MysqlException

file_path = os.path.dirname(__file__)
logging.basicConfig(
    filename="./process.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

def get_mysql_client(user: str, pwd: str, host: str, port: int, db: str) -> MySQLConnection:
  try:
    conn: MySQLConnection = mysql.connector.connect(user=user, password=pwd,
                                  host=host,
                                  port=port,
                                  database=db)
  except mysql.connector.Error as err:
    message = err
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      message = "Something is wrong with your user name or password"
      logging.error(message)
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      message = "Database does not exist"
      logging.error(message)
    else:
      logging.error(err)
    raise MysqlException(message)
  return conn

def process_users_operations(data, report_path: str) -> None:

  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True) 
  for row in tqdm(data):
    user_file: TextIOWrapper = open(file=f'{report_path}/{row[0]}.txt', mode="w")
    user_file.write(row[1])
    user_file.write("\n")
    # operation = f"i"

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
    conn = get_mysql_client(user=args.user, pwd=args.password, host=args.host, port=args.port, db=args.database)
    logging.info('Got mysql client')
  except MysqlException as e:
    sys.exit(e)

  print('Start process')
  logging.info('Start process')
  cursor = conn.cursor()
  cursor.execute("Select * from utenti")
  result = cursor.fetchall()
  today = datetime.today()
  today = today.strftime('%d-%m-%Y')
  report_path = os.path.join(file_path, './reports', today)
  pathlib.Path(report_path).mkdir(parents=True, exist_ok=True)
  process_users_operations(data=result, report_path=report_path)

