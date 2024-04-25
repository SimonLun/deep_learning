
import queue
import threading
import numpy as np

from conf.Paras import para
from conf.base import logger
from utils import MysqlClient, read_sql_file, get_trading_days, move_x_calendar_day, split_by_n, move2_last_busday, \
	is_trading_day, move2_next_busday, convert_to_int, get_date_list, convert_to_datetime, replace
import pandas as pd


class WindSqlEngine(object):
	mysql_db_conn = MysqlClient(database_name='wind')
	
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
	
	def _get_px(self, sql0, q=None, start_date=None, end_date=None):
		if start_date is None:
			start_date = self.start_date
		if end_date is None:
			end_date = self.end_date
		sql = sql0 + ' where TRADE_DT>="{0}" and TRADE_DT<="{1}" '.format(str(start_date), str(end_date))
		data = self.mysql_db_conn.read_sql(sql)
		if q is None:
			return data
		else:
			q.put(data)
	
	def get_active_stocks_df(self, startdate=None, enddate=None, flag=None) -> pd.DataFrame:
		"""获取指定范围内的所有股票"""
		# 这个位置是取得最新的
		if not startdate:
			startdate = self.start_date
		if not enddate:
			enddate = self.end_date
		sql_stocks = read_sql_file("sql_daily_stocks_info")
		info_stocks = self.mysql_db_conn.read_sql(sql_stocks)
		univ_stocks = pd.DataFrame()  # Univ
		busy_days = get_trading_days(str(startdate), str(enddate))
		# StockInfo
		for each_trading_day in busy_days:
			daily_stocks = info_stocks.loc[
				(info_stocks['ipo_date'] <= int(each_trading_day)), ['ticker', 'delist_date', 'exchange']]
			daily_stocks = daily_stocks.loc[
				daily_stocks['delist_date'].fillna(21000101).astype(int) > int(each_trading_day),
				['ticker', 'exchange']]
			daily_stocks['Date'] = each_trading_day
			univ_stocks = pd.concat([univ_stocks, daily_stocks], axis=0)
		univ_stocks.rename(columns={'ticker': 'Ticker'}, inplace=True)
		univ_stocks['Date'] = univ_stocks['Date'].astype(str)
		univ_stocks['exchange_code'] = univ_stocks['exchange'].map(para['exchange_code'])
		univ_stocks['ticker'] = univ_stocks['Ticker'].str.cat(univ_stocks['exchange_code'], sep='.')
		univ_stocks.set_index(['Date', 'Ticker'], inplace=True)
		# change values in column of "exchange" by {'SSE': 1, 'SZSE': 2, 'BSE': 3}
		univ_stocks['exchange1'] = univ_stocks['exchange'].map({'SSE': 1, 'SZSE': 2, 'BSE': 3})
		univ_stocks['All'] = 1
		
		univ_stocks['All_ex_BJ'] = 0
		univ_stocks.loc[univ_stocks['exchange1'] != 3, 'All_ex_BJ'] = 1
		del univ_stocks['exchange1']
		susp_status = self.get_suspension_status(startdate, enddate)
		st_status = self.get_st_stocks_status(startdate, enddate)
		univ_stocks = univ_stocks.join(st_status, how='left').join(susp_status, how='left')
		univ_stocks['Investable_ex_BJ'] = 0
		univ_stocks.loc[(univ_stocks['All_ex_BJ'] == 1) & (univ_stocks['STFlag'].isna()) & (
			univ_stocks['SuspensionFlag'].isna()), 'Investable_ex_BJ'] = 1
		del univ_stocks['STFlag']
		del univ_stocks['SuspensionFlag']
		##判断
		if flag is not None:
			return univ_stocks
		new_issue = self.get_new_issue()
		univ_stocks = univ_stocks.join(new_issue, how='left')
		univ_stocks['Investable_ex_BJ_ipo60'] = 0
		univ_stocks.loc[
			(univ_stocks['Investable_ex_BJ'] == 1) & (univ_stocks['ipo_60d_flag'].isna()), 'Investable_ex_BJ_ipo60'] = 1
		univ_stocks['Investable_ex_BJ_ipo90'] = 0
		univ_stocks.loc[
			(univ_stocks['Investable_ex_BJ'] == 1) & (univ_stocks['ipo_90d_flag'].isna()), 'Investable_ex_BJ_ipo90'] = 1
		# 获取 Marketcap 数据
		float_2000_caps = self.get_Float2000_market_cap(startdate, enddate)
		
		univ_stocks = univ_stocks.join(float_2000_caps, how='left')
		univ_stocks['Float2000'] = univ_stocks['Float2000'].fillna(0).astype(int)
		columns_all = ['ticker', 'All', 'All_ex_BJ', 'Investable_ex_BJ', 'Investable_ex_BJ_ipo60',
		               'Investable_ex_BJ_ipo90',
		               'Float2000']
		# univ_stocks.reset_index(drop=False)
		return univ_stocks[columns_all]
	
	def get_st_stocks_status(self, start_date=None, end_date=None):
		"""st 状态查询
		S: 特别处理(ST)
		Z: 暂停上市
		P: 特别转让服务(PT)
		L: 退市整理
		X: 创业板暂停上市风险警示
		T: 退市
		R: 恢复上市
		Y: *ST"""
		if start_date is None:
			start_date = int(self.start_date)
		if end_date is None:
			end_date = int(self.end_date)
		st_sql = read_sql_file("sql_wind_st_stocks_in_days").format(start_date, end_date)
		stat_df = self.mysql_db_conn.read_sql(st_sql)
		stat_df = stat_df.loc[(stat_df['S_TYPE_ST'] != 'R') & (stat_df['S_TYPE_ST'] != 'T'), :]
		date_all = get_trading_days(start_date, end_date)
		date_all = [int(i) for i in date_all]
		
		res = pd.DataFrame()
		for i in stat_df.index:
			# filter out the st stocks in the days before the st status
			if stat_df.loc[i].REMOVE_DT == 21000101:
				# filter out the date after the st status removed
				date = [dt for dt in date_all if dt >= max(stat_df.loc[i].ENTRY_DT, int(start_date))]
			else:
				#  date = date_all[(date_all >= max(ST.loc[i].ENTRY_DT, startdt)) & (date_all < ST.loc[i].REMOVE_DT)]
				date = [dt for dt in date_all if
				        (dt >= max(stat_df.loc[i].ENTRY_DT, int(start_date))) and (dt < stat_df.loc[i].REMOVE_DT)]
			
			index = pd.MultiIndex.from_product([date, [stat_df.loc[i].Ticker]], names=['Date', 'Ticker'])
			date = pd.DataFrame([1] * len(date), columns=['STFlag'])
			date.index = index
			res = pd.concat([res, date], axis=0)
		res.reset_index(inplace=True)
		res['Date'] = res['Date'].astype(str)
		res = res.groupby(['Date', 'Ticker']).sum()
		res['STFlag'] = 1
		return res.sort_index()
	
	def get_suspension_status(self, start_date=None, end_date=None):
		# 盘中临时停牌且开盘前就公布了的认为是停牌
		# 停牌一天和今起停牌全部认为是停牌
		if start_date is None:
			start_date = self.start_date
		if end_date is None:
			end_date = self.end_date
		
		suspension_sql = read_sql_file("sql_wind_suspension_status_in_days").format(start_date, end_date)
		
		data = self.mysql_db_conn.read_sql(suspension_sql)
		data['Date'] = data['Date'].astype(str)
		
		return data.set_index(['Date', 'Ticker']).sort_index()
	
	def get_new_issue(self):
		move_x_start_date = move_x_calendar_day(self.start_date, -90)
		suspension_sql = read_sql_file("sql_wind_new_issuse").format(move_x_start_date, self.end_date)
		suspension_data = self.mysql_db_conn.read_sql(suspension_sql)
		
		suspension_data['ipo_90'] = [move_x_calendar_day(x, 90) for x in suspension_data['ipo_date']]
		suspension_data['ipo_60'] = [move_x_calendar_day(x, 60) for x in suspension_data['ipo_date']]
		
		ipo_date = suspension_data['ipo_date'].unique()
		
		ret_df = pd.DataFrame()
		for dt in ipo_date:
			ipo_60 = suspension_data.loc[suspension_data['ipo_date'] == dt, 'ipo_60'].values
			ipo_90 = suspension_data.loc[suspension_data['ipo_date'] == dt, 'ipo_90'].values
			ticker = suspension_data.loc[suspension_data['ipo_date'] == dt, 'ticker'].values
			filter_days_ipo_60 = [int(x) for x in self.busy_days if int(x) >= dt and int(x) <= ipo_60[0]]
			index = pd.MultiIndex.from_product([filter_days_ipo_60, ticker],
			                                   names=['Date', 'Ticker'])
			res60 = pd.DataFrame([1] * len(index), columns=['ipo_60d_flag'])
			res60.index = index
			filter_days_ipo_90 = [int(x) for x in self.busy_days if int(x) >= dt and int(x) <= ipo_90[0]]
			index = pd.MultiIndex.from_product([filter_days_ipo_90, ticker], names=['Date', 'Ticker'])
			res90 = pd.DataFrame([1] * len(index), columns=['ipo_90d_flag'])
			res90.index = index
			res90 = res90.merge(res60, left_index=True, right_index=True, how='outer').fillna(0)
			ret_df = pd.concat([ret_df, res90], axis=0)
		ret_df['ipo_60d_flag'] = ret_df['ipo_60d_flag'].astype(int)
		ret_df.reset_index(inplace=True)
		ret_df['Date'] = ret_df['Date'].astype(str)
		ret_df.set_index(['Date', 'Ticker'], inplace=True)
		
		return ret_df.sort_index()
	
	def get_basic_price(self, start_date=None, end_date=None):
		# 14 columns  ['close', 'preclose', 'open', 'high', 'low', 'volume', 'amount', 'vwap',
		#        'pct_chg', 'high_limit', 'low_limit', 'fq', 'trading_status','dailyret']
		if start_date is None:
			start_date = self.start_date
		if end_date is None:
			end_date = self.end_date
		
		update_columns_in_clickhouse = ['close', 'preclose', 'open', 'high', 'low', 'volume', 'amount', 'vwap',
		                                'pct_chg', 'high_limit', 'low_limit', 'fq', 'trading_status']
		fields_ref_in_wind = {
			'close': 'S_DQ_CLOSE as close', 'preclose': 'S_DQ_PRECLOSE as preclose',
			'open': 'S_DQ_OPEN as open', 'high': 'S_DQ_HIGH as high',
			'low': 'S_DQ_LOW as low', 'volume': 'S_DQ_VOLUME*100 as volume',
			'amount': 'S_DQ_AMOUNT/10 as amount', 'vwap': 'S_DQ_AVGPRICE as vwap',
			'pct_chg': 'S_DQ_PCTCHANGE/100 as pct_chg', 'high_limit': 'S_DQ_LIMIT as high_limit',
			'low_limit': 'S_DQ_STOPPING as low_limit',
			'fq': 'S_DQ_ADJFACTOR as fq',
			'trading_status': 'S_DQ_TRADESTATUSCODE as trading_status'}
		field_ref_by_wind = [fields_ref_in_wind[i] for i in update_columns_in_clickhouse]
		wind_ashare_price_sql = f"""select cast(TRADE_DT as signed) as Date, LEFT(S_INFO_WINDCODE, 6) as Ticker, {','.join(field_ref_by_wind)}  from wind.ASHAREEODPRICES """
		if len(self.busy_days) < 360:
			# single process
			px_data = self._get_px(wind_ashare_price_sql, start_date=start_date, end_date=end_date)
		else:
			# multi process
			# # get active stocks
			threads_list = []
			q_queue = queue.Queue()
			trading_days_list = split_by_n(self.busy_days, n=10)
			for each_period_days in trading_days_list:
				t = threading.Thread(target=self._get_px,
				                     args=(each_period_days[0], each_period_days[-1], wind_ashare_price_sql, q_queue))
				threads_list.append(t)
			[i.start() for i in threads_list]
			[i.join() for i in threads_list]
			data_init = pd.DataFrame()
			for i in threads_list:
				px_data = pd.concat([data_init, q_queue.get()], axis=0)
		
		px_data.loc[:, 'Date'] = px_data['Date'].astype(str)
		px_data.set_index(['Date', 'Ticker'], inplace=True)
		px_data.sort_index(inplace=True)
		
		return px_data
	
	def get_daily_ret(self, px_data):
		Ret = px_data['close'] * px_data['fq']
		Ret = Ret.unstack().sort_index()
		DRet = Ret / Ret.shift(1) - 1
		Ret = px_data['pct_chg'].unstack().sort_index()
		DRet[DRet.isna()] = Ret[DRet.isna()]
		px_data = px_data.merge(pd.DataFrame(DRet.stack(), columns=['dailyret']), left_index=True, right_index=True,
		                        how='left')
		return px_data
	
	def get_Float2000_market_cap(self, start_date=None, end_date=None):
		# if Float2000 in ticker:
		if start_date is None:
			start_date = self.start_date
		if end_date is None:
			end_date = self.end_date
		new_start_date = self.start_date
		# 获取前面5天的交易日
		for i in range(5):
			new_start_date = str(move2_last_busday(new_start_date))
		market_cap_sql = read_sql_file("sql_wind_market_amount").format(new_start_date, end_date)
		market_cap = self.mysql_db_conn.read_sql(market_cap_sql)
		market_cap['Date'] = market_cap['Date'].astype(str)
		market_cap = market_cap.set_index(['Date', 'Ticker']).sort_index()[["amount"]]
		stock_pool = self.get_active_stocks_df(new_start_date, end_date, flag=True)
		market_cap = stock_pool.join(market_cap, how='left')
		market_cap = pd.DataFrame(market_cap['amount'])
		market_cap = market_cap.unstack()
		market_cap = market_cap.rolling(window=5).mean().shift(1)
		market_cap = market_cap.stack()
		market_cap.reset_index(inplace=True)
		market_cap = market_cap.loc[market_cap.groupby('Date')['amount'].rank(ascending=False) <= 2000, :]
		market_cap.set_index(['Date', 'Ticker'], inplace=True)
		market_cap['amount'] = 1
		market_cap.columns = ['Float2000']
		return market_cap
	
	def get_industry_code_in_citics_lv1(self):
		"""获取中证一级行业"""
		sql = read_sql_file("sql_citics_lv1")
		citics_lv1 = self.mysql_db_conn.read_sql(sql)
		citics_lv1 = citics_lv1.set_index(['Date', 'Ticker']).sort_index()[["citics_lv1"]]
		return citics_lv1
	
	def get_relist_info(self):
		relist_sql = read_sql_file("sql_wind_relist_stocks")
		relist = self.mysql_db_conn.read_sql(relist_sql)
		return relist
	
	def get_tick_dateinfo(self):
		"""INFO1 参数 """
		ticker_info_sql = read_sql_file("sql_wind_a_capital_on_a_share_freefloat")
		# Info1,Date0 = 变动日期,Date1=变动日期(上市日)
		info_stocks = self.mysql_db_conn.read_sql(ticker_info_sql.format(self.start_date, self.end_date))
		info_stocks['Date0'] = [move2_last_busday(x) for x in info_stocks['Date1']]
		info_stocks['Date'] = [x if is_trading_day(x) else move2_next_busday(x) for x in info_stocks['Date']]
		return info_stocks
	
	def get_stock_price(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		stock_price_sql = read_sql_file("sql_wind_prices")
		stock_price = self.mysql_db_conn.read_sql(stock_price_sql.format(start_date, end_date))
		return stock_price
	
	def get_stock_mktcap(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		stock_mktcap_sql = read_sql_file("sql_wind_mktcap")
		stock_mktcap = self.mysql_db_conn.read_sql(stock_mktcap_sql.format(start_date, end_date))
		return stock_mktcap
	
	def get_industry_code(self) -> pd.DataFrame:
		# 1 column: IndustryCode
		# df0 = pd.DataFrame()
		# for dt in self.busy_days:
		wind_industrycode_sql = read_sql_file("sql_wind_industrycode")
		wind_indu = self.mysql_db_conn.read_sql(wind_industrycode_sql)
		# df_tmp = pd.DataFrame(wind_indu)
		# if wind_indu.empty:
		# 	logger.error(f'sql_wind_industrycode:empty data!')
		# 	return 0
		# df0 = pd.concat([df0, df_tmp], axis=0)
		try:
			wind_indu.loc[:, 'IndustryCode'] = replace(wind_indu['IndustryCode'], para['industry_index_1'],
			                                           para['industry_index_1_hash'])
		except Exception as err:
			logger.error(err)
		return wind_indu
	
	def get_pct_chg(self, stardate, enddate):
		sql_pct_chg = read_sql_file("sql_get_pct_chg").format(stardate, enddate)
		pct_chg = self.mysql_db_conn.read_sql(sql_pct_chg).set_index(['Date', 'Ticker']).loc[:, "pct_chg"].unstack()
		return pct_chg
	
	def get_index_weight_v2(self, startdate, enddate, ticker=['000300.SH', '000905.SH', '000852.SH']):
		
		wind_index_weight_month_sql = read_sql_file("sql_wind_index_weight_monthly").format("','".join(ticker),
		                                                                                    convert_to_int(startdate),
		                                                                                    convert_to_int(enddate))
		IdxWgt = self.mysql_db_conn.read_sql(wind_index_weight_month_sql)
		IdxWgt = IdxWgt.loc[IdxWgt['Ticker'] != '689009', :]
		return IdxWgt
	
	def norm_yaxis_to_1(self, data, type=1):
		if type == 1:
			# maxtrix
			Total = pd.DataFrame(data.sum(axis=1), columns=['sum'])
			Total = 1 / Total
			Total = Total[['sum'] * len(data.columns)]
			Total.columns = data.columns
			data = data * Total
		elif type == 2:
			# dataframe with multi-columns
			Col = data.columns
			Data = pd.DataFrame()
			for col in Col:
				data1 = data[[col]].unstack()
				data1 = self.norm_yaxis_to_1(data1, type=1).stack()
				if Data.shape[0] == 0:
					Data = data1
				else:
					Data = Data.merge(data1, left_index=True, right_index=True, how='outer')
			
			data = Data
		
		return data
	
	def get_index_member(self, ticker, start_date=None, end_date=None, datelist=None):
		# get member for one index ticker
		if datelist is None:
			datelist = get_trading_days(start_date, end_date)
		else:
			datelist = convert_to_int(datelist)
		
		if len(datelist) <= 10:
			# for loop
			res = pd.DataFrame()
			for dt in datelist:
				wind_index_member_sql = read_sql_file("sql_wind_index_member").format(str(dt), ticker)
				index_member = self.mysql_db_conn.read_sql(wind_index_member_sql)
				res = pd.concat([res, index_member], axis=0)
		else:
			# multi-process
			# ts = []
			# q = queue.Queue()
			res = pd.DataFrame()
			for dt in datelist:
				wind_index_member_sql = read_sql_file("sql_wind_index_member").format(str(dt), ticker)
				index_member = self.mysql_db_conn.read_sql(wind_index_member_sql)
				res = pd.concat([res, index_member], axis=0)
			# t = threading.Thread(target=_get_index_member, args=(ticker, dt, q))
			# ts.append(t)
		
		return res.sort_values(['Date', 'Ticker'])
	
	def get_consensusexpectation_factor(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		consensusexpectation_factor_sql = read_sql_file("sql_wind_consensus_expectation_factor")
		consensusexpectation_factors = self.mysql_db_conn.read_sql(
			consensusexpectation_factor_sql.format(start_date, end_date))
		return consensusexpectation_factors
	
	def get_ashare_moneyflow(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		ashare_moneyflow_sql = read_sql_file("sql_wind_ashare_moneyflow")
		ashare_moneyflow_factors = self.mysql_db_conn.read_sql(ashare_moneyflow_sql.format(start_date, end_date))
		return ashare_moneyflow_factors
	
	def get_zhongzheng_index(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		zz_index_sql = read_sql_file("sql_wind_zz500_index")
		zz500_index = self.mysql_db_conn.read_sql(zz_index_sql.format(start_date, end_date))
		return zz500_index
	
	def get_uplimit_stocks(self, start_date=None, end_date=None):
		if not start_date:
			start_date = self.start_date
		if not end_date:
			end_date = self.end_date
		uplimit_stocks_sql = read_sql_file("sql_wind_uplimit_stocks")
		uplimit_stocks = self.mysql_db_conn.read_sql(uplimit_stocks_sql.format(start_date, end_date))
		return uplimit_stocks


if __name__ == '__main__':
	wind_conn = WindSqlEngine('20200101', '20230101')
	uplimit_stocks = wind_conn.get_uplimit_stocks()
	zz500_index = wind_conn.get_zhongzheng_index()
