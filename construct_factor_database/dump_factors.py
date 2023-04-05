import os
import pandas as pd

from conf.base import logger
from get_factors_from_sql import get_all_factors
from conf.Paras import para



def dump_factors_to_parquet():
    for i in range(10):
        factors = get_all_factors(para['start_date_list'][i], para['end_date_list'][i])
        factors.to_parquet(os.path.join(para['parquet_path'],f"factors_{para['start_date_list'][i][:4]}.parquet"))

def read_parquet(kind=None):
    df_all = pd.DataFrame()
    for i in range(10):
        year = para['start_date_list'][i][:4]
        df = pd.read_parquet(os.path.join(para['parquet_path'],f"factors_{year}.parquet"))
        logger.info(f" success to get factors_{year}.parquet")
        df.set_index(['Ticker','Date'],inplace=True)
        if kind:
            df = df[para[kind]]
        df = df.astype('float32')
        df_all = pd.concat([df_all,df],axis=0,ignore_index=False)
    logger.info(f" success to get {kind} factor ,shape = {df_all.shape}")

    return df_all
    
        

if __name__ == '__main__':
    for factor_cate in ['expectation_factor_list','moneyflow_factor_list']:
        factor_df = read_parquet(factor_cate)
        a = factor_df.isna().sum()
        a.to_csv(os.path.join(para['parquet_path'], f'{factor_cate}.csv'))
    