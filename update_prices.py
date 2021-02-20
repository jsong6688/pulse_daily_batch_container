import yfinance as yf
import mysql.connector
from mysql.connector.constants import ClientFlag
import datetime
import pandas as pd
from configparser import ConfigParser
import os
import sys

#change cwd to script directory and read config file
os.chdir(os.path.dirname(sys.argv[0]))
setting=ConfigParser()
setting.read('setting.ini')

# print( setting['ssl location']['ssl_ca'])
def update_prices(rundate):
    #create db connection to google cloud
    config = {
        'user': setting['db config']['user'],
        'password': setting['db config']['password'],
        'host': setting['db config']['host'],
        'database':setting['db config']['database'],
        'client_flags': [ClientFlag.SSL],
        'ssl_ca': setting['ssl location']['ssl_ca'],
        'ssl_cert': setting['ssl location']['ssl_cert'],
        'ssl_key': setting['ssl location']['ssl_key']
    }


    conn_cloud = mysql.connector.connect(**config)
    cursor_cloud = conn_cloud.cursor()

    security_map = {'AUDUSD Curncy':'AUDUSD=X',
                'EURUSD Curncy': 'EURUSD=X',
                'GBPUSD Curncy':'GBPUSD=X',
                'NZDUSD Curncy':'NZDUSD=X',
                'CADUSD Curncy':'CADUSD=X',
                'CHFUSD Curncy':'CHFUSD=X',
                'JPYUSD Curncy':'JPYUSD=X',
                'CNHUSD Curncy':'CNHUSD=X',
                'SEKUSD Curncy':'SEKUSD=X',
                'NOKUSD Curncy':'NOKUSD=X',
                'TRYUSD Curncy':'TRYUSD=X',
                'ZARUSD Curncy':'ZARUSD=X',
                'MXNUSD Curncy':'MXNUSD=X',
                'SGDUSD Curncy':'SGDUSD=X'

    }

    #determine the latest date db is updated

    cursor_cloud.execute("select max(effective_date) from prices")
    start_date=cursor_cloud.fetchall()[0][0].strftime("%Y-%m-%d")

    #Get data from yahoo API
    security_str = ' '.join([x for x in security_map.values()])
    data = yf.download(security_str, start=start_date, end=rundate.strftime("%Y-%m-%d"),
                       group_by="ticker",interval='60m')

    #change output time to NY time as FX close is 5pm NY always.
    from dateutil import tz
    to_zone = tz.gettz('America/New_York')
    data.index=data.index.tz_convert(to_zone)

    #grab eod price only 5pm NY time
    data=data[(data.index.hour==16) & (data.index.minute==0)]

    #re-organize data into upload format
    upload=[]
    for security in security_map.keys():
        data_single=data.iloc[:, data.columns.get_level_values(0)==(security_map[security])].fillna(method='ffill')
        data_single.columns = data_single.columns.droplevel() # drop Adj_close

        for index, row in data_single.iterrows():
            effective_date=index.date()
            price=row['Close']
            upload.append([effective_date,security,'FX', float(price)])

    #Delete existing data in db that is in the same date range as upload list, allows multiple runs without dupes
    #Mutilple runs fails when there is missing rates for a particular ccy on a given day, which should be rare.

    min_date=min(date[0] for date in upload)
    max_date=max(date[0] for date in upload)

    sqlstr = """Delete from prices where effective_date >= %s
             and effective_date<=%s """
    cursor_cloud.execute(sqlstr, [min_date.strftime("%Y-%m-%d"),max_date.strftime("%Y-%m-%d")])
    conn_cloud.commit()

    #upload the rates

    sqlstr= ("INSERT INTO prices (effective_date, bbg_code, asset_class, value) "
             "VALUES (%s, %s, %s, %s)")


    cursor_cloud.executemany(sqlstr, upload)
    conn_cloud.commit()  # and commit changes

    print('Upload successful')
