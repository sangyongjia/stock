#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
from tornado import gen
import logging
import instock.core.stockfetch as stf
import instock.core.statistics.visualization as vis
import instock.core.statistics.fetch_stistics_data as fsd
import instock.web.base as webBase
import pandas as pd
import datetime
import random
__author__ = 'syj '
__date__ = '2024/8/22 '


def fetch_data_from_db():  
    # 这里使用SQLAlchemy作为示例，你需要根据你的数据库进行配置  
    start_date = datetime.datetime.now()  
    dates = pd.date_range(start=start_date, periods=10)  # 使用pandas的date_range函数生成日期序列  
    
    # 生成随机的温度值，这里我们假设温度在-10到40度之间  
    temperatures = [random.randint(-10, 40) for _ in range(10)]  
    
    # 将日期和温度组合成DataFrame 
    data = {'year': dates, 'birth_rate': temperatures}  
    df = pd.DataFrame(data)  
    return df 

# 获得页面数据。
class GetDataStatisticsHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        try:
            #table_name = self.get_argument("table_name", default=None, strip=False)
            date = self.get_argument("date", default=None, strip=False)
            ##name = self.get_argument("name", default=None, strip=False)
            comp_list = []
            data = fsd.fetch_enter_statistics()
            script, div= vis.create_plot(date,data)
            comp_list.append({"script": script, "div": div})


        except Exception as e:
            logging.error(f"dataStatisticsHandler.GetDataStatisticsHandler处理异常：{e}")

        self.render("my_dynamic_plot.html", comp_list=comp_list,
                     leftMenu=webBase.GetLeftMenu(self.request.uri))


