import datetime
import os
import warnings
from utils import format_date
from conf.base import logger
from construct_factor_database.hermes_db_result import HermesSqlEngine
from construct_factor_database.wind_db_result import WindSqlEngine
from conf.Paras import para
import pandas as pd

warnings.filterwarnings('ignore')


def get_factors_from_hermes(start_date, end_date):
	hermes_conn = HermesSqlEngine(start_date, end_date)
	hermes_quant_factor = hermes_conn.get_hermes_quant_factor()
	hermes_quant_factor['exchange_code'] = hermes_quant_factor['secID'].str[-4:]
	hermes_quant_factor['exchange_code'] = hermes_quant_factor['exchange_code'].map(para['exchange_code'])
	hermes_quant_factor['ticker'] = hermes_quant_factor['ticker'].str.cat(hermes_quant_factor['exchange_code'], sep='.')
	del hermes_quant_factor['secID']
	del hermes_quant_factor['exchange_code']
	hermes_quant_factor.rename(columns={'tradeDate': 'Date', 'ticker': 'Ticker'}, inplace=True)
	hermes_quant_factor['Date'] = hermes_quant_factor['Date'].apply(lambda x: format_date(x))
	return hermes_quant_factor


def get_factors_from_wind(start_date, end_date):
	wind_conn = WindSqlEngine(start_date, end_date)
	active_stocks = wind_conn.get_active_stocks_df()
	active_stocks = active_stocks[active_stocks['Investable_ex_BJ_ipo90'] == 1].reset_index()
	active_stocks = active_stocks[['ticker', 'Date']]
	active_stocks.rename(columns={ 'Date': 'date'}, inplace=True)
	
	consensusexpectation_factor = wind_conn.get_consensusexpectation_factor()
	ashare_moneyflow = wind_conn.get_ashare_moneyflow()
	
	consensusexpectation_factor = pd.merge(active_stocks, consensusexpectation_factor, on=['ticker', 'date'],
	                                       how='left')
	ashare_moneyflow = pd.merge(active_stocks, ashare_moneyflow, on=['ticker', 'date'], how='left')
	wind_factors = pd.merge(consensusexpectation_factor, ashare_moneyflow, on=['ticker', 'date'])
	wind_factors.rename(columns={'ticker': 'Ticker', 'date': 'Date'}, inplace=True)
	return wind_factors


def get_all_factors(start_date, end_date):
	logger.info(f"start to get sql data")
	wind_factor = get_factors_from_wind(start_date, end_date)
	logger.info(f"get wind factor from sql,shape = {wind_factor.shape}")
	hermes_factor = get_factors_from_hermes(start_date, end_date)
	logger.info(f"get hermes factor from sql,shape = {hermes_factor.shape}")
	factors_all = pd.merge(hermes_factor, wind_factor, on=['Ticker', 'Date'],how='right')
	logger.info(f"get all factors from mysql database,shape = {factors_all.shape}")
	
	return factors_all

def dump_factors_to_parquet():
	for i in range(10):
		factors = get_all_factors(para['start_date_list'][i], para['end_date_list'][i])
		factors.to_parquet(os.path.join(para['parquet_path'], f"factors_{para['start_date_list'][i][:4]}.parquet"))


def read_parquet(kind=None):
	df_all = pd.DataFrame()
	for i in range(10):
		year = para['start_date_list'][i][:4]
		df = pd.read_parquet(os.path.join(para['parquet_path'], f"factors_{year}.parquet"))
		logger.info(f" success to get factors_{year}.parquet")
		df.set_index(['Ticker', 'Date'], inplace=True)
		if kind:
			df = df[para[kind]]
		df = df.astype('float32')
		df_all = pd.concat([df_all, df], axis=0, ignore_index=False)
	logger.info(f"success to get {kind} factor ,shape = {df_all.shape}")
	return df_all

		
def get_indu_code_from_sql(start_date, end_date):
	wind_conn = WindSqlEngine(start_date, end_date)
	stock_indu = wind_conn.get_industry_code()
	return stock_indu

def get_stock_price_from_sql(start_date,end_date):
	wind_conn = WindSqlEngine(start_date, end_date)
	stock_price = wind_conn.get_stock_price()
	return stock_price

def get_stock_mktcap_from_sql(start_date,end_date):
	wind_conn = WindSqlEngine(start_date, end_date)
	stock_mktcap = wind_conn.get_stock_mktcap()
	return stock_mktcap