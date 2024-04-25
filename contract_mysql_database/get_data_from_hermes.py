# -*- coding: utf-8 -*-
import datetime
import http.client
import os
import time
import traceback
import urllib

import gzip
from io import BytesIO

import pandas as pd

HTTP_OK = 200
HTTP_AUTHORIZATION_ERROR = 401
from sqlalchemy import create_engine


class Client:
    domain = 'api.wmcloud.com'
    port = 443
    token = ''
    # 设置因网络连接，重连的次数
    reconnectTimes = 2
    httpClient = None

    def __init__(self):
        self.httpClient = http.client.HTTPSConnection(self.domain, self.port, timeout=60)

    def __del__(self):
        if self.httpClient is not None:
            self.httpClient.close()

    def encodepath(self, path):
        # 转换参数的编码
        start = 0
        n = len(path)
        re = ''
        i = path.find('=', start)
        while i != -1:
            re += path[start:i + 1]
            start = i + 1
            i = path.find('&', start)
            if (i >= 0):
                for j in range(start, i):
                    if (path[j] > '~'):
                        re += urllib.parse.quote(path[j])
                    else:
                        re += path[j]
                re += '&'
                start = i + 1
            else:
                for j in range(start, n):
                    if (path[j] > '~'):
                        re += urllib.parse.quote(path[j])
                    else:
                        re += path[j]
                start = n
            i = path.find('=', start)
        return re

    def init(self, token):
        self.token = token

    def getData(self, path):
        result = None
        path = '/data/v1' + path
        print(path)
        path = self.encodepath(path)
        for i in range(self.reconnectTimes):
            try:
                # set http header here
                self.httpClient.request('GET', path, headers={"Authorization": "Bearer " + self.token,
                                                              "Accept-Encoding": "gzip, deflate"})
                # make request
                response = self.httpClient.getresponse()
                result = response.read()
                compressedstream = BytesIO(result)
                gziper = gzip.GzipFile(fileobj=compressedstream)
                try:
                    result = gziper.read()
                except:
                    pass
                return response.status, result
            except Exception as e:
                if i == self.reconnectTimes - 1:
                    raise e
                if self.httpClient is not None:
                    self.httpClient.close()
                self.httpClient = http.client.HTTPSConnection(self.domain, self.port, timeout=60)
        return -1, result


def get_date_list(start_dt, end_dt):
    # get trade_days
    date_all = pd.read_csv('../local_mktTradeDays.csv')
    date_list = list(date_all['date'])
    date_list = [str(i) for i in date_list if i >= start_dt and i <= end_dt]
    return date_list
    # while prev_trade_date not in trade_date:
    #     tmp = prev_trade_datetime - datetime.timedelta(1)
    #     prev_trade_date = tmp.strftime('%Y%m%d')
    #     prev_trade_datetime = datetime.datetime.strptime(prev_trade_date, '%Y%m%d')


if __name__ == "__main__":
    try:
        # mysql engine
        engine = create_engine('mysql+pymysql://root:lunyaqi@localhost:3306/hermes')

        client = Client()
        client.init('360eb047375a0a89de7e592881675726bc2432c63d403f28d7e399caaf02a101')
        # 方式1，直接调取数据，不做任何处理
        date_list = get_date_list(20110829, 20150101)
        for dt in date_list:
            time.sleep(1)
            url1 = f"/api/market/getMktStockFactorsOneDayPro.json?field=&secID=&ticker=&tradeDate={dt}"
            code, result = client.getData(url1)  # 调用getData函数获取数据，数据以字符串的形式返回
            if code == 200:
                # print(result.decode('utf-8',errors='replace'))#url1须为json格式，才可使用utf-8编码
                if eval(result)['retCode'] == 1:
                    pd_data = pd.DataFrame(eval(result)['data'])
                    pd_data.to_sql('hermes_quant_factor', engine, schema=None,if_exists='append', index=False)
                    # 将数据转化为DataFrame格式
                    print(pd_data)
                else:
                    print(result.decode('utf-8', errors='replace'))
            else:
                print(code)
                print(result)
    except Exception as e:
        # traceback.print_exc()
        raise e
