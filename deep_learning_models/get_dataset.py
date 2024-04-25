#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2023/4/9 15:17
# @Author  : lunyq
# @FileName: get_dataset.py
# @Software: PyCharm
import os
import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
from conf.Paras import para
warnings.filterwarnings("ignore")

def get_feature_from_npy():
	df = pd.DataFrame()
	features = {}  # 存储每个因子的数据
	for factor_name in para['final_factor_list']:
		single_factor = np.load(os.path.join(para['factor_npy_path'], f"{factor_name}.npy"))
		df = pd.concat([df, pd.DataFrame(single_factor)], axis=1)
		# features[factor_name] = single_factor.reshape((-1, 1, 1))  # 将数据转换成形状为 (n, 1, 1) 的张量
	# X = np.concatenate(list(features.values()), axis=1)
	# 将每个因子的张量堆叠成一个输入张量，形状为 (n, 18, 1, 1)
	#
	idx = pd.read_parquet(os.path.join(para['parquet_path'], f"idx.parquet"))
	df = pd.concat([idx, df], axis=1)
	df['Date'] = pd.to_datetime(df['Date'])
	price_data = set_labels()
	X = pd.merge(df,price_data,how='left',on=['Date','Ticker'])
	# x_predict = X.drop(columns=['label','close'])
	# x_predict.dropna(inplace=True)
	# X.dropna(inplace=True)
	# Y = np.array(X['label'])
	# X=np.array(X[0])
	return X

def calculate_return(group):
	# 将收盘价向前平移五行
	close_prices = group['close'].shift(-5)
	# 计算收益率
	group["return_5"] = ((close_prices - group['close']) / group['close']).round(3)
	return group


def set_labels():
	df = pd.read_parquet(os.path.join(para['parquet_path'], f"stock_price.parquet"))
	df = df.sort_values(by=['Ticker', 'Date'])
	df['Date'] = pd.to_datetime(df['Date'])
	df.set_index('Date', inplace=True)
	
	# 按照股票代码分组
	groups = df.groupby('Ticker')
	
	# 计算未来 5 日收益率
	data = groups.apply(calculate_return)
	quantiles = data.groupby("Date")["return_5"].quantile([0.1, 0.45, 0.55, 0.9]).unstack().round(3)
	
	data["label"] = np.nan
	for dt in tqdm(quantiles.index):
		quantiles_dt = quantiles.loc[dt]
		data.loc[dt, "label"] = np.where(data.loc[dt, "return_5"] > quantiles_dt[0.9], 1,
		                                 np.where(data.loc[dt, "return_5"] < quantiles_dt[0.1], -1, np.where(
			                                 (data.loc[dt, "return_5"] > quantiles_dt[0.45]) & (
						                                 data.loc[dt, "return_5"] < quantiles_dt[0.55]), 0, np.nan)))
	df.reset_index(inplace=True)
	return data


if __name__ == '__main__':
	x,y=get_feature_from_npy()
	print(x.shape,y.shape)