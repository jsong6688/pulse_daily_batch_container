import sys
import os
from configparser import ConfigParser
import mysql.connector
from mysql.connector.constants import ClientFlag
#change cwd to script directory and read config file
os.chdir(os.path.dirname(sys.argv[0]))
setting=ConfigParser()
setting.read('setting.ini')
sys.path.append(setting['file location']['Func_path'])

from Pulse_module import Pulse

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

# This is for writting Pulse score to DB using SQLAlchmy engine for Pandas
con_str = "mysql+pymysql://{}:{}@{}/{}".format(setting['db config']['user'],
            setting['db config']['password'], setting['db config']['host'],
            setting['db config']['database']) # Connection string for sql alchmy
ssl_args = {'ssl':{'cert': setting['ssl location']['ssl_cert'],
                            'key': setting['ssl location']['ssl_key'],
                            'ca':setting['ssl location']['ssl_ca'],
                            'check_hostname':False}}

def update_score():

    pulse = Pulse(conn_str = config)
    pulse.generate_idx_all_ccy(con_str=con_str, ssl_args=ssl_args)
