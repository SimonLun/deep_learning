from conf.Paras import para
from conf.base import logger
from utils import MysqlClient, read_sql_file, get_trading_days, move_x_calendar_day, split_by_n, move2_last_busday, \
	is_trading_day, move2_next_busday, convert_to_int, get_date_list, convert_to_datetime, replace
import pandas as pd



class HermesSqlEngine(object):
	mysql_db_conn = MysqlClient(database_name='hermes')

	def __init__(self, start_date, end_date):
		self.start_date = start_date
		self.end_date = end_date
		self.busy_days = self.get_trading_days()

	def get_trading_days(self, s_start_date=None, s_end_date=None):
		# 读取的npy 需要
		if s_start_date is None:
			s_start_date = self.start_date
		if s_end_date is None:
			s_end_date = self.end_date
		return get_trading_days(s_start_date, s_end_date)
	
	def get_hermes_quant_factor(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		hermes_quant_factor_sql = read_sql_file("sql_hermes_quant_factor")
		hermes_quant_factor = self.mysql_db_conn.read_sql(hermes_quant_factor_sql.format(start_date, end_date))
		return hermes_quant_factor
