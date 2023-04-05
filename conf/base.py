import logging
import os
from logging.handlers import TimedRotatingFileHandler
from os.path import dirname, abspath

MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'lunyaqi'
MYSQL_DATABASE = 'hermes'


# SQL DIR PATH
parent_path = dirname(dirname(abspath(__file__)))
sqls_path = os.path.join(parent_path, 'sqls')

base_path = os.path.join(parent_path, 'logs')
if not os.path.exists(base_path):
    os.makedirs(base_path)

logger = logging.getLogger('deep_learning')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

filehandler_datacube = TimedRotatingFileHandler(os.path.join(base_path,'deep_learning.log'), 'D', 10, 100)
filehandler_datacube.setLevel(logging.DEBUG)
filehandler_datacube.setFormatter(formatter)
logger.addHandler(filehandler_datacube)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)