# Purpose of this code is to attempt to access economic calendar data from Investing.com

#########################################################################
# Set up dependencies / packages / settings
#########################################################################

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime #Module to handle str / num time variables
import time as time
from bs4 import BeautifulSoup #Beautiful soup to handle webpage content
import pandas as pd #Pandas df
import re as re #Regular expression
import sys #import system settings
import os
import mysql.connector
from mysql.connector.constants import ClientFlag
from configparser import ConfigParser

#change cwd to script directory and read config file
os.chdir(os.path.dirname(sys.argv[0]))
setting=ConfigParser()
setting.read('setting.ini')

#establish database connection
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

# now we establish our connection
conn_cloud = mysql.connector.connect(**config)
cursor_cloud = conn_cloud.cursor()

#reset #Clear all vars - this command technically reset only works in Jupyter / ipython...

def scrap_eco_data(startdate,rundate):
    options = Options()
    options.add_argument("--disable-notifications")
    options.add_argument("--start_maximized")
    driver = webdriver.Chrome(setting['file location']['chromedriver']) #Establish a Chrome driver
    driver.get("https://www.investing.com/economic-calendar/") #Access the designated webpage using driver


    # current_date=datetime.datetime.now().date()
    # rundate= (current_date-datetime.timedelta(days=1) if current_date.weekday()!=0 else current_date-datetime.timedelta(days=3)).strftime("%m/%d/%Y")

    # Define a dict for various key global settings
    settings = {"start_dt": startdate, "end_dt":rundate,
                "Mapping_path": setting['file location']['Mapping_path'],
                "Func_path": setting['file location']['Func_path'],
                "Update_Cty_Map": False}
    # Finally add main path / mapping path and import user defined functions
    sys.path.append(settings["Func_path"])
    from Investing_eco_tools import get_country_mapping, soup_get_cur, soup_get_importance, soup_get_event, soup_get_reading, soup_get_release_time #Import udf
    from Investing_formatting_tools import format_release_dt,format_data_release,format_effective_dt,format_country_to_ccy,format_event_name

    #########################################################################
    # Module 0: Import various mappings from local, map to HTML identifiers
    ##########################################################################

    # Import current filter settings (Set locally)
    filter_map = pd.read_excel(io = settings["Mapping_path"] + "\FilterMapping.xlsx",
        engine="openpyxl", sheet_name = "current") #Read current mapping

    # First deal with country mapping - generate new mapping if out of date - funcion embedded in this script
    if settings["Update_Cty_Map"] == True:
        get_country_mapping(settings["Mapping_path"]) #If updating then run code to designated path
    country_map = pd.read_csv(settings["Mapping_path"] + "\CountryMapping.csv") #Read country map

    # Generate 'Country'' code mapping via left join
    country_filter  = pd.merge(filter_map.loc[:, 'Country'].to_frame(name = "Country_Name"), country_map,  on = "Country_Name")

    # Generate 'Time' filter mapping and merge
    time_to_html = pd.DataFrame([["Time Only", "timetimeOnly"], ["Time Reamining", "timeFilterRemain"]], columns = ["Time", "Time_html_id"])
    time_filter = pd.merge(filter_map.loc[:, 'Time'].to_frame(name = "Time"), time_to_html, on = "Time")

    time_filter.columns
    # Generate 'Category code mapping to html identifier
    category_to_html = pd.DataFrame([["All", "selectAll('category[]');"], ["Employment", "category_employment"], ["Credit", "category_credit"], ["Balance", "category_balance"],
                                        ["Economic Activity", "category_economicActivity"], ["Central Bank", "category_centralBanks"], ["Bonds", "category_Bonds"],
                                            ["Inflation", "category_inflation"], ["Confidence Index", "category_confidenceIndex"]], columns=["Category", "Category_html_id"])

    category_filter = pd.merge(filter_map.loc[:, 'Category'].to_frame(name = "Category"), category_to_html, on = "Category")

    # Generate 'Importance' flag code mapping to html identifier
    importance_to_html = pd.DataFrame([["All", ["importance1", "importance2", "importance3"]], ["Importance 1", "importance1"], ["Importance 2", "importance2"], ["Importance 3", "importance3"]],
                                        columns = ["Importance", "Importance_html_id"])
    importance_filter = pd.merge(filter_map.loc[:, 'Importance'].to_frame(name = "Importance"), importance_to_html, on = "Importance")

    #########################################################################
    # Module 1: Dealing with Investing.com filters
    #########################################################################
    for i in range(0, 10):
        while True:
            try:
                #################################
                ## Handle filters first
                #################################

                adv_filters_btn = driver.find_element_by_id("filterStateAnchor")
                driver.execute_script("arguments[0].click();", adv_filters_btn)

                ## Conutry Filter sub-modules

                # Clear all existing filters
                country_clear_all_btn = driver.find_element_by_css_selector("a[onclick*=\"clearAll('country[]'\"")
                driver.execute_script("arguments[0].click();", country_clear_all_btn)

                # Apply country filters
                for i in range(len(country_filter)):
                    country_btn_temp = driver.find_element_by_id("country" + str(country_filter.loc[i, "Country_Code"]))
                    driver.execute_script("arguments[0].click();", country_btn_temp)

                ## Time Filter sub-modules
                time_filter_btn = driver.find_element_by_id(time_filter.iloc[0, 1])
                driver.execute_script("arguments[0].click();", time_filter_btn)

                ## Category Filter sub-modules

                # Clear all existing filters
                category_clear_all_btn = driver.find_element_by_css_selector("a[onclick*=\"clearAll('category[]'\"")
                driver.execute_script("arguments[0].click();", category_clear_all_btn)

                category_filter_btn = driver.find_element_by_css_selector("a[onclick*=\"" + category_filter.iloc[0, 1])
                driver.execute_script("arguments[0].click();", category_filter_btn)

                ## Importance Filter sub-modules
                for a in importance_filter["Importance_html_id"]:
                    #print(a)
                    driver.execute_script("arguments[0].click();", driver.find_element_by_id(a))
                    #driver.find_element_by_id(a).click()

                # Apply filter
                driver.execute_script("arguments[0].click();", driver.find_element_by_id("ecSubmitButton"))

                # Clean up - explicit wait
                WebDriverWait(driver, 10).until(
                    EC.invisibility_of_element((By.ID, "calendarFilterBox_country")))

                #################################
                ## Handle start / end dates
                #################################
                driver.execute_script("arguments[0].click();", driver.find_element_by_id("datePickerToggleBtn"))

                # Locate start & end date input objects, update and click
                ''' Update is sequantial because the elements refresh after clearing / inputing
                so you will need to find element again after the 'start_dt' value is updated.

                Also JAVA Script executioner doesn't work here - you need to emulate sending keys.. otherwise doesn't update! '''

                driver.execute_script("window.scrollTo(0, 500);")

                start_dt_input = driver.find_element_by_id("startDate")
                start_dt_input.clear()
                start_dt_input.send_keys(settings["start_dt"])

                end_dt_input = driver.find_element_by_id("endDate")
                end_dt_input.clear()
                end_dt_input.send_keys(settings["end_dt"])

                driver.execute_script("arguments[0].click();", driver.find_element_by_id("applyBtn"))

                break #If executed fully then continue to the next block

            except:
                ####### Need to build out exception handling for robustness
                ####### Common causes are - pop ups / advertisments etc.


                # Scroll to the bottom of the page
                #driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 1500);")
                # signup_cls = driver.find_element_by_css_selector(".popupCloseIcon.largeBannerCloser")
                # signup_cls.click()
                pass

            continue
        break

    #########################################################################
    # Module 2: Extract data from BeautifulSoup / HTML structure
    #########################################################################

    ###################################################################
    #First handle table size (scroll down until full table is visible)
    ##################################################################

    #driver.get_window_size()
    # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # At the moment scroll to full height seem to do the job. if breaks in the future
    # will need to test size of table dynamically changing

    ###################################################################
    #Now extract into beautifulsoup content for extraction (Could take a while!)
    ##################################################################

    # Check if the HTML table in Soup contains start & end date in the row header

    missing = True #Start with missing and run while loop
    counter = 0
    while missing:
        full_soup = BeautifulSoup(driver.page_source,'html5lib')
        time_str_lst = []
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        for dt_str in full_soup.findAll('td', {"class" : "theDay"}):
            time_str_lst.append(dt_str.get_text())
        missing = not(datetime.datetime.strftime(datetime.datetime.strptime(settings['start_dt'], "%m/%d/%Y"), "%A, %B %#d, %Y") in time_str_lst and datetime.datetime.strftime(datetime.datetime.strptime(settings['end_dt'], "%m/%d/%Y"), "%A, %B %#d, %Y") in time_str_lst)
        counter += 1
        time.sleep(3)
        if counter >= 10:
            raise Exception("Soup unable to find start date and end date in HTML source code. Please debug")

    # Filters to extract data - use regular expression to find all rows with eventRowID_XXX

    table_soup = full_soup.findAll('tr', id = re.compile('^eventRowId'))

    # HTML tree structure
    # Currency:
    # class = 'left flagCur noWrap'
    # span, text or just class.text?
    # Importance:
    # class = 'left textNum sentiment noWrap'
    # data-img-key content
    # Event:
    # class = 'left event'
    # a.text
    # Actual value = contains 'eventActual_' .text / content
    # Forecast value = contains 'eventForecast_' .text / contents
    # Previous value = contains s'eventPrevious_' .text /contents

    table_cur = []
    table_importance = []
    table_event = []
    table_reading = []
    table_release_time = []

    soup_get_release_time(table_soup[0])
    for rows in table_soup:
        # Extract currency
        try:
            table_cur.append(soup_get_cur(rows))
        except:
            table_cur.append('ERR')

        # Extract importance
        try:
            table_importance.append(soup_get_importance(rows))
        except:
            table_importance.append('ERR')

        # Extract event name
        try:
            table_event.append(soup_get_event(rows))
        except:
            table_event.append('ERR')

        # Extract value, reading
        try:
            table_reading.append(soup_get_reading(rows))
        except:
            table_reading.append('ERR')

        # Extract release time
        try:
            table_release_time.append(soup_get_release_time(rows))
        except:
            table_release_time.append('ERR')

        # Compile pandas df
        table_df = pd.DataFrame(
                {'Currency': table_cur,
                 'Importance': table_importance,
                 'Event': table_event,
                 'Reading': table_reading,
                 'Release_Time': table_release_time}
                 )

    # table_df.to_csv(path_or_buf=settings["Out_path"] + "Beta_test_output.csv") #Finally export
    print("Web Scrapper Successful!")



    #Read from mapping file
    data_list_mapping = pd.DataFrame(pd.read_excel(io = settings["Mapping_path"] + "\data_list_invest_dot_com.xlsx",
                    sheet_name = "data_list", engine="openpyxl",))
    #Create formatted data as new columns
    formatted_df=table_df[table_df['Release_Time']!='N/A']
    formatted_df.loc[:,'Release_Time_Upload']=formatted_df.Release_Time.apply(lambda x: format_release_dt(x))
    formatted_df.loc[:,'Actual_Upload']=formatted_df.Reading.apply(lambda x: format_data_release(x))
    formatted_df.loc[:,'Effective_Date_Upload']=formatted_df.Event.apply(lambda x: format_effective_dt(x))
    formatted_df.loc[:,'Investing_Name']=formatted_df.Event.apply(lambda x: format_event_name(x))
    formatted_df.loc[:,'country']=formatted_df.Currency.apply(lambda x: format_country_to_ccy(x))

    #Merge mapping xlsx with scrapped data
    mapped_df=pd.merge(formatted_df,data_list_mapping,on=['Investing_Name','country'],how='left').dropna(subset=['bbg_code'])
    mapped_df.loc[:,'Actual_Upload']=mapped_df['Actual_Upload'] * mapped_df['Scaling']

    #Create df for upload
    upload_df=mapped_df.loc[:,['data_name','bbg_code','country','sector','freq','Effective_Date_Upload','Release_Time_Upload','Actual_Upload','score_direction']]
    #upload_df['Release_Time_Upload'] = pd.to_datetime(upload_df['Release_Time_Upload'])


    #Delete existing data that clashes with date range of upload df

    min_date=min(upload_df.Release_Time_Upload).strftime("%Y-%m-%d")
    max_date=max(upload_df.Release_Time_Upload).strftime("%Y-%m-%d")

    sqlstr = """Delete from eco_data where release_date >= %s
             and release_date<=%s """
    cursor_cloud.execute(sqlstr, [min_date,max_date])
    conn_cloud.commit()

    #upload into google cloud
    sql = "INSERT INTO eco_data (data_name,bbg_code,country,sector,freq,effective_date,release_date,value,score_direction) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor_cloud.executemany(sql, upload_df.values.tolist())

    conn_cloud.commit()

    print('Data Uploaded!')

    driver.quit()
