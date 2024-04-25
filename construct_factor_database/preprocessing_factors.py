import os
import numpy as np
import alphalens
import pandas as pd
from tqdm import tqdm

from conf.Paras import para
from conf.base import logger
from construct_factor_database.get_factors_from_sql import read_parquet, get_indu_code_from_sql, \
	get_stock_price_from_sql, get_stock_mktcap_from_sql
from utils import filter_extreme_MAD, standardize, fill_missing_values, neutralize_df
from sklearn import preprocessing
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

matplotlib.use('TkAgg')
font = {'family': 'SimHei', "size": 12}
matplotlib.rc('font', **font)


def check_data_nan():
	for factor_cate in para['factor_category']:
		factor_df = read_parquet(factor_cate)
		a = factor_df.isna().sum()
		a.to_csv(os.path.join(para['parquet_path'], f'{factor_cate}.csv'))


def get_stock_price_and_stock_mktcap():
	logger.info('start to get stock price ')
	stock_price = get_stock_price_from_sql(start_date, end_date)
	logger.info('succ to get stock price ')
	stock_price['Date'] = stock_price['Date'].astype(str)
	stock_price.to_parquet(os.path.join(para['parquet_path'], f"stock_price.parquet"))
	logger.info('start to get stock mktcap ')
	stock_mktcap = get_stock_mktcap_from_sql(start_date, end_date)
	logger.info('succ to get stock mktcap ')
	stock_mktcap['Date'] = stock_mktcap['Date'].astype(str)
	stock_mktcap.to_parquet(os.path.join(para['parquet_path'], f"stock_mktcap.parquet"))


def single_factor_report(single_factor, stock_mktcap, stock_price, factor_cate, factor_name, factor_report):
	if factor_cate in ['momentum_factor_list', 'volatility_factor_list', 'turnover_factor_list',
	                   'sentiment_factor_list', 'technical_factor_list', 'expectation_factor_list',
	                   'moneyflow_factor_list']:
		single_factor = fill_missing_values(single_factor, factor_name, type=1)
	# 同行业填充
	elif factor_cate in ['value_factor_list']:
		single_factor = fill_missing_values(single_factor, factor_name, type=3)
	# 插值填充
	elif factor_cate in ['growth_factor_list', 'financial_factor_list', 'psi_factor_list']:
		single_factor = fill_missing_values(single_factor, factor_name, type=4)
	# 离群值处理
	single_factor[factor_name] = filter_extreme_MAD(single_factor[factor_name], n=5)
	# 中性化处理
	
	single_factor = pd.merge(single_factor, stock_mktcap, how='left', on=['Date', 'Ticker'])
	single_factor[factor_name] = neutralize_df(single_factor, factor_name, industry_col='IndustryCode',
	                                           mkt_cap_col='mkt_cap')
	# z-score标准化
	single_factor[factor_name] = standardize(single_factor[factor_name])
	# 因子有效性检验
	single_factor['Date'] = pd.to_datetime(single_factor['Date'])
	single_factor.set_index(['Date', 'Ticker'], inplace=True)
	
	del single_factor['mkt_cap']
	
	factor_npy = single_factor[factor_name].to_numpy()
	np.save(os.path.join(para['factor_npy_path'], f"{factor_name}.npy"), factor_npy)
	factor_data = alphalens.utils.get_clean_factor_and_forward_returns(single_factor[factor_name],
	                                                                   prices=stock_price,
	                                                                   groupby=single_factor[
		                                                                   'IndustryCode'],
	                                                                   binning_by_group=False,
	                                                                   quantiles=5,
	                                                                   periods=(1, 5, 10),
	                                                                   groupby_labels=para['industry_dict']
	                                                                   )
	
	mean_quant_ret, std_quantile = alphalens.performance.mean_return_by_quantile(factor_data)
	
	mean_quant_rateret = mean_quant_ret.apply(alphalens.utils.rate_of_return, axis=0,
	                                          base_period=mean_quant_ret.columns[0])
	
	mean_quant_ret_bydate, std_quant_daily = alphalens.performance.mean_return_by_quantile(factor_data, by_date=True,
	                                                                                       by_group=False)
	
	mean_quant_rateret_bydate = mean_quant_ret_bydate.apply(alphalens.utils.rate_of_return, axis=0,
	                                                        base_period=mean_quant_ret_bydate.columns[0], )
	
	compstd_quant_daily = std_quant_daily.apply(alphalens.utils.std_conversion, axis=0,
	                                            base_period=std_quant_daily.columns[0])
	
	alpha_beta = alphalens.performance.factor_alpha_beta(factor_data)
	
	mean_ret_spread_quant, std_spread_quant = alphalens.performance.compute_mean_returns_spread(
		mean_quant_rateret_bydate, factor_data["factor_quantile"].max(), factor_data["factor_quantile"].min(),
		std_err=compstd_quant_daily, )
	
	ic = alphalens.performance.factor_information_coefficient(factor_data)
	ic_npy = ic['5D'].to_numpy()
	np.save(os.path.join(para['factor_npy_path'], f"{factor_name}_ic.npy"), ic_npy)
	# 记录写入df
	# 分组信息
	quantile_tb = alphalens.plotting.plot_quantile_statistics_table(factor_data)
	quantile_tb.to_csv(os.path.join(para['single_factor_report_path'], f"{factor_name}_quantile.csv"))
	# 收益
	return_tb = alphalens.plotting.plot_returns_table(alpha_beta, mean_quant_rateret, mean_ret_spread_quant)
	return_tb.to_csv(os.path.join(para['single_factor_report_path'], f"{factor_name}_return.csv"))
	# ic
	ic_tb = alphalens.plotting.plot_information_table(ic)
	ic_tb.to_csv(os.path.join(para['single_factor_report_path'], f"{factor_name}_ic.csv"))
	
	factor_report = factor_report.append(pd.Series(
		{'IC_mean': ic_tb.iloc[0, 1], 'IC_std': ic_tb.iloc[1, 1], 'IR': ic_tb.iloc[0, 1] / ic_tb.iloc[1, 1],
		 'alpha': return_tb.iloc[0, 1], 'beta': return_tb.iloc[1, 1]}, name=factor_name))
	return factor_report


def generage_factor_report():
	final_factors = {}
	# check 每个因子原始数据的nan值,存到
	# check_data_nan()
	stock_indu = get_indu_code_from_sql(start_date, end_date)
	# get_stock_price_and_stock_mktcap()
	stock_price = pd.read_parquet(os.path.join(para['parquet_path'], f"stock_price.parquet"))
	stock_mktcap = pd.read_parquet(os.path.join(para['parquet_path'], f"stock_mktcap.parquet"))
	stock_mktcap = fill_missing_values(stock_mktcap, 'mkt_cap', 2)
	stock_mktcap.reset_index(inplace=True)
	idx = pd.read_parquet(os.path.join(para['parquet_path'], f"idx.parquet"))
	stock_price = pd.merge(idx, stock_price, how='left', on=['Date', 'Ticker'])
	stock_price = fill_missing_values(stock_price, 'close', 2)
	stock_price.reset_index(inplace=True)
	stock_price['Date'] = pd.to_datetime(stock_price['Date'])
	stock_price = stock_price.pivot(index='Date', columns='Ticker', values='close')
	stock_price = stock_price.fillna(method='bfill')
	stock_price = stock_price.fillna(method='ffill')
	# 填充行情缺失值
	for factor_cate in para['factor_category']:
		factor_report = pd.DataFrame()
		final_factors[factor_cate] = []
		factor_df = read_parquet(factor_cate)
		for factor_name in tqdm(para[factor_cate]):
			single_factor = factor_df[factor_name]
			if factor_cate != 'expectation_factor_list' and single_factor.isna().sum() / len(single_factor) > 0.2:
				logger.error(f"缺失值数大于20%，剔除该因子:{factor_name}")
			else:
				single_factor = single_factor.reset_index()
				
				# 特殊值 COR 689009.SH
				single_factor.drop(single_factor[single_factor['Ticker'] == '689009.SH'].index, inplace=True)
				# 缺失值处理
				single_factor = pd.merge(single_factor, stock_indu, how='left', left_on='Ticker',
				                         right_on='S_CON_WINDCODE')
				del single_factor['S_CON_WINDCODE']
				single_factor.sort_values(by=['Date', 'Ticker'], inplace=True)
				# 用每只股票各自前后10天的数据均值填充
				try:
					factor_report = single_factor_report(single_factor, stock_mktcap, stock_price, factor_cate,
					                                     factor_name, factor_report)
				except Exception as e:
					logger.error(e)
					continue
				final_factors[factor_cate].append(factor_name)
		factor_report.to_csv(os.path.join(para['single_factor_report_path'], f"{factor_cate}_report.csv"))
	final_factors = pd.DataFrame(final_factors)
	final_factors.to_csv(os.path.join(para['single_factor_report_path'], f"factor_list.csv"))


def filter_factors():
	'''
	根据IC\IR\ALPHA,beta筛选因子
	采用打分法
	筛选条件：
	IC>=0.2
	IR>=0.3
	ALPHA>=0.04
	beta>=-0.04
	:return:
	'''
	final_factors = {}
	for factor_cate in para['factor_category']:
		factor_report = pd.read_csv(os.path.join(para['single_factor_report_path'], f"{factor_cate}_report.csv"))
		factor_report['rank'] =0
		factor_report.loc[(factor_report['IC_mean'] >= 0.02), 'rank'] += 1
		factor_report.loc[(factor_report['IR'] >= 0.03), 'rank'] += 1
		factor_report.loc[(factor_report['alpha'] >= 0.03), 'rank'] += 1
		factor_report.loc[(factor_report['beta'] >= -0.04), 'rank'] += 1
		factor_report = factor_report.loc[(factor_report['rank'] == max(factor_report['rank'])),:]
		factor_report.to_csv(os.path.join(para['single_factor_report_path'], f"{factor_cate}_filtered_report.csv"))
		ic_all = []
		for i in factor_report.iloc[:, 0].values.tolist():
			ic_npy = np.load(os.path.join(para['factor_npy_path'], f"{i}_ic.npy"))
			ic_all.append(ic_npy)
		ic_all = pd.DataFrame(ic_all, index=factor_report.iloc[:, 0].values.tolist()).T
		ic_corr = ic_all.corr()
		plt.subplots(figsize=(13, 13))
		sns.heatmap(ic_corr, annot=True, vmax=1, vmin=0, square=True, cmap='Blues')
		plt.savefig(os.path.join(para['single_factor_report_path'], f"{factor_cate}_corr.png"))
	# 根据相关性热力图手动筛选，剔除相关性>0.7的因子，保留得分较高的因子
	# 得到final factor list


if __name__ == '__main__':
	start_date = '20130101'
	end_date = '20230101'
	filter_factors()
	