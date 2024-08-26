#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import instock.lib.database as mdb
import instock.core.tablestructure as tbs
import logging
import datetime

def fetch_enter_statistics():
    table_name = tbs.TABLE_CN_STOCK_STRATEGIES_STATIC['name']
    date = now_date = datetime.datetime.now().date()
    sql = f"SELECT * FROM `{table_name}` WHERE `date` <= '{date}' "
    try:
        data = pd.read_sql(sql=sql, con=mdb.engine())
        if data is None or len(data.index) == 0:
            return
        return data
    except Exception as e:
            logging.error(f"fetch_enter_statistics处理异常：{e}")