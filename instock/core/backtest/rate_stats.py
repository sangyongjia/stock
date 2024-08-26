#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import numpy as np
import pandas as pd

__author__ = 'myh '
__date__ = '2023/3/10 '


def get_rates(code_name, data, stock_column, threshold=101):
    try:
        # 增加空判断，如果是空返回 0 数据。
        if data is None:
            return None

        start_date = code_name[0]
        code = code_name[1]
        # 设置返回数组。
        stock_data_list = [start_date, code]

        mask = (data['date'] >= start_date)
        data = data.loc[mask].copy()
        data = data.head(n=threshold)

        if len(data.index) <= 1:
            return None

        ##计算“当天战法-1日开盘涨幅”
        data['one_day_open_change_rate'] = np.around(100*((data['open'] - data['close'].shift(1))/data['close'].shift(1)).shift(-1),decimals=2)
        ## “当天战法-2日开盘卖出收益率”
        data['two_day_open_change_rate'] = np.around(100*((data['open'] - data['open'].shift(1))/data['open'].shift(1)).shift(-2),decimals=2)
        data['one_day_open_change_rate'].values[np.isnan(data['one_day_open_change_rate'].values)] = -1.0
        data['two_day_open_change_rate'].values[np.isnan(data['two_day_open_change_rate'].values)] = -1.0

        ##计算“隔天战法-1日开盘涨幅”
        data['getian_one_day_open_change_rate'] = np.around(100*((data['open'] - data['close'].shift(1))/data['close'].shift(1)).shift(-2),decimals=2)
        ## “隔天战法-2日开盘卖出收益率”
        data['getian_two_day_open_change_rate'] = np.around(100*((data['open'] - data['open'].shift(1))/data['open'].shift(1)).shift(-3),decimals=2)
        data['getian_one_day_open_change_rate'].values[np.isnan(data['getian_one_day_open_change_rate'].values)] = -1.0
        data['getian_two_day_open_change_rate'].values[np.isnan(data['getian_two_day_open_change_rate'].values)] = -1.0

        close1 = data.iloc[0]['close']
        # data.loc[:, 'sum_pct_change'] = data['close'].apply(lambda x: round(100 * (x - close1) / close1, 2))
        data.loc[:, 'sum_pct_change'] = np.around(100 * (data['close'].values - close1) / close1, decimals=2)

        # 计算区间最高、最低价格

        first = True
        col_len = len(data.columns)
        for row in data.values:
            if first:
                first = False
            else:
                stock_data_list.append(row[col_len-1])

        _l = len(stock_column) - len(stock_data_list)-4
        for i in range(0, _l):
            stock_data_list.append(None)
        ##当天战法
        stock_data_list.append(data.iloc[0, -5])
        stock_data_list.append(data.iloc[0, -4])
        ##隔天战法
        stock_data_list.append(data.iloc[0, -3])
        stock_data_list.append(data.iloc[0, -2])
    except Exception as e:
        logging.error(f"rate_stats.get_rates处理异常：{code}代码{e}")

    return pd.Series(stock_data_list, index=stock_column)
