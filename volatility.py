# -*- coding: utf-8 -*-
"""
Created on Sat May  4 17:27:09 2019

@author: Aditya Raj
"""

import pandas as pd
import math


dfs = pd.read_excel('returns.xlsx', sheet_name='user_portfolio')
dfs['volatility'] = dfs.PortfolioValue / dfs.PortfolioValue.shift(1)
#dfs['volatility'].fillna(0, inplace=True)
dfs['volatility'] = dfs['volatility'].apply(lambda x : math.pow(math.log(x), 2))
dfs['RollM'] = dfs['volatility'].rolling(window=90,min_periods=0).mean()