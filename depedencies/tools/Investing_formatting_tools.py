''' This is has various UDFs to format table_df output into
    format that is compatible with existing codes
'''

import re
import datetime as dt

'''This is a function that formats the Release column of the Investing_eco_scrapping df output'''
def format_release_dt(string):
    release_time=dt.datetime.strptime(string, "%Y/%m/%d %H:%M:%S")
    release_date=release_time.date()
    return release_date

'''This is a function that formats the Reading column of the Investing_eco_scrapping df output'''
def format_data_release(string):
    try:
        actual=float(re.findall((r"^-?\d*\.{0,1}\d+"), string[0].replace(',', ''))[0]) #replaces comma in the instances of 1,000 for example, then use regex to obtain numbers within the string
    except:
        actual=None
    return actual

def format_effective_dt(string):
    def last_day_of_month(any_day):
        next_month = any_day.replace(day=28) + dt.timedelta(days=4)
        return next_month - dt.timedelta(days=next_month.day)

    month_keys = {"Jan": 1, "Feb":2,"Mar":3,"Apr":4,
                  "May":5,"Jun": 6, "Jul":7,"Aug":8,
                  "Sep":9,"Oct": 10, "Nov":11,"Dec":12,
                  "Q1":3,"Q2": 6, "Q3":9,"Q4":12
                 }
    lastday_keys = {"Jan": 31, "Feb":28,"Mar":31,"Apr":30,
                  "May":31,"Jun": 30, "Jul":31,"Aug":31,
                  "Sep":30,"Oct": 31, "Nov":30,"Dec":31,
                  "Q1":31,"Q2": 30, "Q3":30,"Q4":31
                 }

    try:
        month = re.findall(r"\((\w+)\)", string)[len(re.findall(r"\((\w+)\)", string))-1]
    except:
        month = 'NAN'

    current_month =  dt.datetime.today().month
    try:
        effective_month=month_keys[month]
    except:
        effective_month=dt.datetime.today().month

    if current_month<effective_month:
        effective_year=dt.datetime.today().year-1
    else:
        effective_year=dt.datetime.today().year

    try:
        effective_day=lastday_keys[month]
    except:
        effective_day=last_day_of_month(dt.date(effective_year, effective_month,1)).day

    effective_date=dt.date(effective_year, effective_month,effective_day)
    return effective_date

'''This is a function that formats the Currency column of the Investing_eco_scrapping df output'''
def format_country_to_ccy(string):
    currency_keys = {"Australia": 'AUD', "United Kingdom":'GBP',"United States":'USD',"Canada":'CAD',
                     "Mexico":'MXN',"Sweden": 'SEK', "Italy":'EUR',"South Korea":'KRW',
                     "Switzerland":'CHF',"Germany": 'EUR', "France":'EUR',"Spain":'EUR',"Norway":'NOK',"Belgium":'EUR',
                     "Japan":'JPY',"China": 'CNH', "New Zealand":'NZD',"Turkey":'TRY',"Euro Zone":'EUR',"South Africa":'ZAR',"Singapore":'SGD'
                     }

    try:
        ccy=currency_keys[string]
    except:
        ccy=None
    return ccy

'''This is a function that formats the Event column of the Investing_eco_scrapping df output'''
def format_event_name(string):
    month_keys = {"Jan": 1, "Feb":2,"Mar":3,"Apr":4,
                  "May":5,"Jun": 6, "Jul":7,"Aug":8,
                  "Sep":9,"Oct": 10, "Nov":11,"Dec":12,
                  "Q1":3,"Q2": 6, "Q3":9,"Q4":12
                 }

    pos=string.rfind('(')
#Deals with variations of event name in Invest.com
    try:
        month=month_keys[re.findall(r"\((\w+)\)", string)[len(re.findall(r"\((\w+)\)", string))-1]]
        event=string[0:pos].strip()
    except:
        event=string.strip()
    return event
