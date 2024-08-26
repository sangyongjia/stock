#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import numpy as np

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.singleton_stock import stock_hist_data
from instock.core.stockfetch import fetch_stock_top_entity_data
import datetime

__author__ = 'myh '
__date__ = '2023/3/10 '


def prepare(date, strategy):
    try:
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            return
        table_name = strategy['name']
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date)
        if results is None:
            return

        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns']) 


        data = pd.DataFrame(results)
        
        ## 修订位置
        foreign_key = list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        foreign_key.append("change_rate")
        foreign_key.append("change_rate_norm")
        columns = tuple(foreign_key)
        
        ##给放量上涨表添加信息
        table_name_selection = tbs.TABLE_CN_STOCK_SELECTION['name'] ##表名是 ”cn_stock_selection“
        if not mdb.checkTableIsExist(table_name_selection):
            logging.error(f"表cn_stock_selection不存在")
            pass
        ##临时替代start
        # tmp_year, tmp_month, tmp_day = "2024-08-19".split("-")
        # date_tmp = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
        # if date< date_tmp:
        #     date_ = date_tmp
        # else:
        #     date_ = date
        ##临时替代end
        sql = f'''SELECT code,industry,popularity_rank,style,concept,volume_ratio,turnoverrate,total_market_cap,holder_newest,holder_ratio,allcorp_num,allcorp_fund_num,rank_change,concern_rank_7days,changerate_ty FROM `{table_name_selection}` WHERE `date` = '{date}' '''
        data_selection = pd.read_sql(sql=sql, con=mdb.engine())
        # if table_name == "cn_stock_strategy_enter":
        #     ## 修订位置
        #     foreign_key = list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        #     foreign_key.append("change_rate")
        #     columns = tuple(foreign_key)
        # else:
        #     columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'],)
        #columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'],)

        data.columns = columns
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data_base = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        data = pd.merge(data_base, data_selection, on=['code'], how='inner') 
        data_static = data.groupby(['industry']).size().to_frame(name='industry_num')#.sort_values('industry_num', ascending=False)  
        data = pd.merge(data, data_static, on=['industry'], how='inner')##.sort_values("popularity_rank",ascending=True) 

        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

    except Exception as e:
        logging.error(f"strategy_data_daily_job.prepare处理异常：{strategy}策略{e}")
             

def run_check(strategy_fun, table_name, stocks, date, workers=1): ##workers=40 --> workers=1
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    data = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            if is_check_high_tight:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date, istop=(k[1] in stock_tops)): k for k in stocks}
            else:
                future_to_data={}
                for k in stocks:
                    tmp_k=list(k)
                    df = stocks[k]
                    data_str = date.strftime("%Y-%m-%d")
                    p_change_list = df.loc[df['date']==data_str,'p_change'].values
                    if len(p_change_list) == 0:
                        p_change = 0.0
                    else:
                        p_change = p_change_list[0]

                    if k[1].startswith("30"):
                        p_change_norm = p_change/2
                    else:
                        p_change_norm = p_change

                    ##p_change['p_change'].values[np.isnan(p_change['p_change'].values)] = 0.0
                    tmp_k.append(p_change)
                    tmp_k.append(p_change_norm)
                    final_k = tuple(tmp_k)
                    future_to_data[executor.submit(strategy_fun, k, stocks[k], date=date)]=final_k
            
                # future_to_data={}
                # if table_name == "cn_stock_strategy_enter":
                # #future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date): k for k in stocks}
                #     ##调整
                #     for k in stocks:
                #         tmp_k=list(k)
                #         df = stocks[k]
                #         data_str = date.strftime("%Y-%m-%d")
                #         p_change_list = df.loc[df['date']==data_str,'p_change'].values
                #         if len(p_change_list) == 0:
                #             p_change = 0.0
                #         else:
                #             p_change = p_change_list[0]
                #         ##p_change['p_change'].values[np.isnan(p_change['p_change'].values)] = 0.0
                #         tmp_k.append(p_change)
                #         final_k = tuple(tmp_k)
                #         future_to_data[executor.submit(strategy_fun, k, stocks[k], date=date)]=final_k
                # else:
                #     future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date): k for k in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    if future.result():
                        data.append(stock)
                except Exception as e:
                    logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{table_name}")
    except Exception as e:
        logging.error(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")
    if not data:
        return None
    else:
        return data


def main():
    # 使用方法传递。
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor: ## add max_workers=1
        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            executor.submit(runt.run_with_args, prepare, strategy)


# main函数入口
if __name__ == '__main__':
    main()
