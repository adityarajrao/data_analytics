# -*- coding: utf-8 -*-
"""
Created on Sat Mar 16 15:45:07 2019

@author: Aditya Raj
"""

import sys
import MySQLdb as mdb
import requests
import json
import csv
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from mysql.connector import MySQLConnection, Error
from datetime import datetime, timedelta
import pandas as pd

if __name__ == '__main__':
    try:
        #con = mdb.connect('127.0.0.1',port=3306, user='root',passwd='password',db='db')
        cur = con.cursor(mdb.cursors.DictCursor)

        old_date = (datetime.now() - timedelta(90)).strftime('%Y-%m-%d')
        args = ['2', '0', '0', '2', '0', '0', None, None, 'http://localhost:3001/uploads/', old_date]
        result_args = cur.callproc('leader_list', args)
    

        r = cur.fetchall()
        df = pd.DataFrame.from_records(r)
        df['percentile'] = df.Cummulative.rank(pct=True) * 5
        
    
    except Error as e:
        print(e)
    
    else:
        cur.close()
        con.close()
        
    #insert records into percentile table    
    records_to_insert = []
    today = (datetime.now()).strftime('%Y-%m-%d')
    for index, row in df.iterrows():
        records_to_insert.append((today, row['UserKey'], row['percentile']))
    
    
    try:
        #con = mysql.connector.connect(host='127.0.0.1', database='db', user='root', password='password')
        sql_insert_query = """INSERT INTO percentile_score (ComputingDate, UserKey, Score) values (%s, %s, %s)"""
        cur = con.cursor(prepared=True)
        result  = cur.executemany(sql_insert_query, records_to_insert)
        con.commit()
        print (cur.rowcount, "Record inserted successfully into three_month_percentile_score table")
        
    except mysql.connector.Error as error :
        print("Failed inserting record into python_users table {}".format(error))
        
    else:
        cur.close()
        con.close()
    
    # export data to google sheet
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('ksjfgjh97cfeff.json', scope)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key('su65fds-Jsnsafyb76sa76sgafo7Nucy4fKtYoPY')
    worksheet = sh.get_worksheet(0)
    #values_list = worksheet.row_values(1)
    worksheet.clear()
    print("worksheet cleared")
    set_with_dataframe(worksheet, df)
    print(df)

