#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import numpy as np
import logging
from bokeh.plotting import figure  
from bokeh.models import Range1d  
from bokeh.resources import CDN  
from bokeh.embed import components  
import datetime
from bokeh.layouts import column, row, layout
import json
import pandas as pd

# 图表生成函数  
def create_plot(date, data):  
 
    
    #start = data['date'].min().strftime("%Y-%m-%d")  
    #end = data['date'].max().strftime("%Y-%m-%d") 
    data['date_str'] = data['date'].apply(str).apply(lambda x: x[-5:])
    rg = tuple(data['date_str'].tolist())
    ##x_range=(start,end),,x_axis_type=""
    p = figure(title="涨跌股票数量比", x_axis_label='日期', y_axis_label='涨跌股票数量比',width=1500,height=600, outer_width=10000, outer_height=300,
               min_border_left=80,
               x_range=rg,
               x_axis_type="auto",
               y_range=Range1d(start=0, end=1.0),
               toolbar_location="below",tools="hover", tooltips=[("x", "@x"), ("y", "@y")])  
    
    #print(data["date_str"])
    p.scatter(data['date_str'] , data['PosNegRate'], marker="circle", size=6, color="blue", alpha=0.5, legend_label="PosNegRate")  
    p.line(data['date_str'] , data['PosNegRate'], line_width=2, color='blue')  
    #
    # 添加第二个Y轴（右侧）  
    p.extra_y_ranges = {"PosNegCount": Range1d(start=0, end=205)}  
    #p.add_layout(p.extra_y_axes[0], 'left')
    p.scatter(data['date_str'] , data['PosNegCount'], marker="circle", size=6, color="red", alpha=0.5,legend_label="PosNegCount") 
    p.line(data['date_str'] , data['PosNegCount'],  y_range_name="PosNegCount", line_color="red", line_width=2)  

    ##概念词统计图
    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")  
    # 使用date()方法将datetime对象转换为datetime.date对象  
    date_only = date_obj.date()
    style_str = data.loc[data['date']==date_only].iloc[0]['ConceptStatistics']  
    style_str_dict = json.loads(style_str)
    rows = [{'style': k, 'num': v} for k, v in style_str_dict.items()]
    style_dict_df = pd.DataFrame(rows).sort_values(by='num', ascending=False)
    #style_dict_df.columns = ['style','num']

    p1 = figure(title="概念词统计", x_axis_label='概念词', y_axis_label='概念词出现次数',width=11000,height=600, outer_width=10000, outer_height=300,
            min_border_left=80,
            x_range=style_dict_df['style'].to_list(),
            x_axis_type="auto",
            #y_range=Range1d(start=0, end=1.0),
            toolbar_location="below",tools="hover", tooltips=[("x", "@x"), ("y", "@y")])
    
    #p.vbar(x=fruits, top=counts, width=0.9, color=Category10[10])  
    p1.scatter(style_dict_df['style'] , style_dict_df['num'], marker="circle", size=6, color="blue", alpha=0.5, legend_label="概念词统计")  
    p1.line(style_dict_df['style'] , style_dict_df['num'], line_width=2, color='blue') 
     
    ##策略1、2收益曲线
    p2 = figure(title="收益曲线", x_axis_label='日期', y_axis_label='收益',width=1500,height=600, outer_width=10000, outer_height=300,
               min_border_left=80,
               x_range=rg,
               x_axis_type="auto",
               y_range=Range1d(start=-11000, end=11000),
               toolbar_location="below",tools="hover", tooltips=[("x", "@x"), ("y", "@y")])  
    
    ##策略1
    p2.scatter(data['date_str'] , data['strategy1_money'], marker="circle", size=6, color="blue", alpha=0.5, legend_label="strategy1_money")  
    p2.line(data['date_str'] , data['strategy1_money'], line_width=2, color='blue')  
    ##策略2
    p2.scatter(data['date_str'] , data['strategy2_money'], marker="circle", size=6, color="red", alpha=0.5, legend_label="strategy2_money")  
    p2.line(data['date_str'] , data['strategy2_money'], line_width=2, color='red')  
    ##策略3
    p2.scatter(data['date_str'] , data['strategy3_money'], marker="circle", size=6, color="green", alpha=0.5, legend_label="strategy3_money")  
    p2.line(data['date_str'] , data['strategy3_money'], line_width=2, color='green')
    # 组合图
    layout = column(p,p1,p2) 
        # 组合图
    # layouts = layout(row(
    #     column(
    #         row(children=[div_attention, div_dfcf_hq, div_dfcf_zl, div_dfcf_pr, select_all, select_none],
    #             align='end'),
    #         p_kline,
    #         p_volume, tabs_indicators), ck))
    # script, div = components(layouts)
    


    #p.scatter(data['date_str'] , data['PosNegCount'], marker="circle", size=6, color="blue", alpha=0.5, legend_label="Circle")  
    #p.line(data['date_str'] , data['PosNegCount'],  y_range_name="PosNegCount", line_color="red", line_width=2)  

    ##p.circle(data['date'], data['PosNegRate'], size=30)
    script, div = components(layout,)  
    return script, div  