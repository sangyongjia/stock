#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
import datetime
import time
from collections import Counter 
import json

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.backtest.rate_stats as rate
from instock.core.singleton_stock import stock_hist_data

__author__ = 'myh '
__date__ = '2023/3/10 '


# 股票策略回归测试。
def prepare():
    tables = [tbs.TABLE_CN_STOCK_INDICATORS_BUY, tbs.TABLE_CN_STOCK_INDICATORS_SELL]
    tables.extend(tbs.TABLE_CN_STOCK_STRATEGIES)
    backtest_columns = list(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
    backtest_columns.insert(0, 'code')
    backtest_columns.insert(0, 'date')
    backtest_column = backtest_columns ##['date', 'code', 'rate_1', 'rate_2', 'rate_3', 'rate_4', 'rate_5', 'rate_6', 'rate_7', 'one_day_open_change_rate', 'two_day_open_change_rate', 'getian_one_day_open_change_rate', 'getian_two_day_open_change_rate']

    stocks_data = stock_hist_data().get_data() ##返回所有股票的历史数据，格式为{(date,code,name):该股票历史数据,...,(date,code,name):该股票历史数据}
    if stocks_data is None:
        return
    for k in stocks_data:
        date = k[0]
        break
    # 回归测试表
    with concurrent.futures.ThreadPoolExecutor() as executor:  ## add max_workers=1
        for table in tables:
            executor.submit(process, table, stocks_data, date, backtest_column)


def process(table, data_all, date, backtest_column):
    table_name = table['name']
    if not mdb.checkTableIsExist(table_name):   ##buy 和 sell表没有就返回？
        return

    column_tail = tuple(table['columns'])[-20]
    now_date = datetime.datetime.now().date()
    sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `{column_tail}` is NULL"
    try:
        data = pd.read_sql(sql=sql, con=mdb.engine())
        if data is None or len(data.index) == 0:
            return

        subset = data[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
        # subset['date'] = subset['date'].values.astype('str')
        subset = subset.astype({'date': 'string'})
        stocks = [tuple(x) for x in subset.values]  #此处的stocks是所有需要backtest更新的数据，格式为[(date,code,name),(),...]

        results = run_check(stocks, data_all, date, backtest_column)
        if results is None:
            return

        data_new = pd.DataFrame(results.values())
        mdb.update_db_from_df(data_new, table_name, ('date','code'))

    except Exception as e:
        logging.error(f"backtest_data_daily_job.process处理异常：{table}表{e}")


def run_check(stocks, data_all, date, backtest_column, workers=1): ##workers=40--> workers=1
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_data = {executor.submit(rate.get_rates, stock,
                                              data_all.get((date, stock[1], stock[2])), backtest_column,
                                              len(backtest_column) - 5): stock for stock in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[stock] = _data_
                except Exception as e:
                    logging.error(f"backtest_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
    except Exception as e:
        logging.error(f"backtest_data_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data

def statistics():
    tables = [tbs.TABLE_CN_STOCK_STRATEGIES[0],] ##input tables
    output_table_name = tbs.TABLE_CN_STOCK_STRATEGIES_STATIC['name']  ##output tables
    for table in tables:
        table_name = table['name']
        now_date = datetime.datetime.now().date()
        date_start = (now_date + datetime.timedelta(days=-(30 * 3))) ##统计近三个月的数据

        #sql = f"SELECT * FROM `{table_name}` WHERE `date` < '{now_date}' AND `date` > `{date_start}` groupby "
        sql = f"""select date,NegativeCount,PositiveCount,PositiveCount+NegativeCount as PosNegCount,PositiveCount/(PositiveCount+NegativeCount) as PosNegRate 
        from (select date,SUM(CASE WHEN two_day_open_change_rate <= 0 THEN 1 ELSE 0 END) AS NegativeCount,SUM(CASE WHEN two_day_open_change_rate > 0 THEN 1 ELSE 0 END) AS PositiveCount 
            from {table_name} where date < '{now_date}' AND date > '{date_start}' group by date) t1
            order by date"""
        sql_base = f"""select date,concept from {table_name} WHERE  date <= '{now_date}' AND date > '{date_start}' """
        try:
            ##part1:获取概念的统计数据
            data_base = pd.read_sql(sql=sql_base, con=mdb.engine())
            #word_counts = data1['concept'].str.split(',', expand=True).stack().value_counts()  
            # 初始化一个字典来存储每个日期的词频  
            word_freq_by_date = {}  
            
            # 遍历DataFrame，统计每个日期的词频  
            for _, row in data_base.iterrows():  
                date = row['date']  
                concepts = row['concept'].replace(" ", "").split(',') 
                word_counts = Counter(concepts)  
                
                # 如果日期已经在字典中，则更新其词频  
                if date in word_freq_by_date:  
                    word_freq_by_date[date].update(word_counts)  
                # 否则，添加新的日期和词频  
                else:  
                    word_freq_by_date[date] = word_counts

            word_freq_dict_by_date = {date:json.dumps(dict(counter),ensure_ascii=False) for date, counter in word_freq_by_date.items()}   
            result_df = pd.DataFrame.from_dict(word_freq_dict_by_date, orient='index').reset_index()  
            result_df.columns = ['date', 'ConceptStatistics']  

            ##part2:获取sql数据
            data = pd.read_sql(sql=sql, con=mdb.engine())
            if data is None or len(data.index) == 0:
                return
            
            ##part3:放量+人气策略盈利能力回测
            ##strategy1:"按照日涨跌幅降序排列取前五名"
            ##strategy2:"按照股吧人气排名降序排列取前五名"
            ##strategy2:"在持股机构大于等于3的前提下，按照股吧人气排名降序排列取前五名"
            sql1 = f"""SELECT   
                        date,  
                        SUM(two_day_open_change_rate) AS strategy1_percent,
                        (SUM(two_day_open_change_rate)*10000/100) as strategy1_money
                        
                    FROM (  
                        SELECT   
                            date,  
                            two_day_open_change_rate,  
                            ROW_NUMBER() OVER (PARTITION BY date ORDER BY change_rate DESC) AS rn  
                        FROM   
                            {table_name}  
                    ) AS ranked_data  
                    WHERE   
                        rn <= 5  
                    GROUP BY   
                        date"""
            data1 = pd.read_sql(sql=sql1, con=mdb.engine())
            if data1 is None or len(data1.index) == 0:
                return
            
            sql2 = f"""SELECT   
                        date,  
                        SUM(two_day_open_change_rate) AS strategy2_percent,
                        (SUM(two_day_open_change_rate)*10000/100) as strategy2_money  
                    FROM (  
                        SELECT   
                            date,  
                            two_day_open_change_rate,  
                            ROW_NUMBER() OVER (PARTITION BY date ORDER BY popularity_rank ASC) AS rn  
                        FROM   
                            {table_name}  
                    ) AS ranked_data  
                    WHERE   
                        rn <= 5  
                    GROUP BY   
                        date"""
            data2 = pd.read_sql(sql=sql2, con=mdb.engine())
            if data2 is None or len(data2.index) == 0:
                return
            
            sql3 = f"""SELECT   
                        date,  
                        SUM(two_day_open_change_rate) AS strategy3_percent,
                        (SUM(two_day_open_change_rate)*10000/100) as strategy3_money  
                    FROM (  
                        SELECT   
                            date,  
                            two_day_open_change_rate,  
                            ROW_NUMBER() OVER (PARTITION BY date ORDER BY turnoverrate DESC) AS rn  
                        FROM   
                            {table_name}  
                    ) AS ranked_data  
                    WHERE   
                        rn <= 5  
                    GROUP BY   
                        date"""
            data3 = pd.read_sql(sql=sql3, con=mdb.engine())
            if data3 is None or len(data3.index) == 0:
                return
            
            # 删除老数据。
            if mdb.checkTableIsExist(output_table_name):
                del_sql = f"DELETE FROM `{output_table_name}` where `date` <= '{now_date}'"
                mdb.executeSql(del_sql)
                cols_type = None
            else:
                cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES_STATIC['columns'])

            ##part1 ,part2 and part3 merge
            data = pd.merge(data, result_df, on=['date'], how='inner') 
            data = pd.merge(data, data1, on=['date'], how='inner')
            data = pd.merge(data, data2, on=['date'], how='inner')
            data = pd.merge(data, data3, on=['date'], how='inner')
            mdb.insert_db_from_df(data, output_table_name, cols_type, False, "`date`")

            ##mdb.update_db_from_df(data, output_table_name, ('date',))
        except Exception as e:
            logging.error(f"backtest_data_daily_job.statistics{table_name}表{e}")
def main():
    prepare()
    statistics()


# main函数入口
if __name__ == '__main__':
    _start = datetime.datetime.now()
    start = time.time()
    print("########backtest单独执行开始时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    main()
    print("######## 完成任务, 使用时间: %s 秒 #######" % (time.time() - start))
