'''
Pulse Module v0.02
J.Song
This module contains all key methods and dependencies required for the production of the Pulse economic indicator.
'''

class Pulse:

    '''
    Pulse is the primary class object handling all Pulse index related tasks, from SQL read/write, index calculation/aggregation and its associated analytics.

    Attributes
    ----------
    unique_ccy_full : list
        a list of of all currencies we cover
    default_sector_w : dictionary
        default mapping of sector to weight
    conn_str : str
        PYOBDC connection string
    conn_flag : int
        1 = Connected / 0 = Not connected to sql engine
    weight_tbl : pands.core.df
        Pandas dataframe of the weights for each indicator at release_dates
    index_tbl : pandas.core.df
        detailed table of index contribution with all the columns and attributes retained
    index_ts_agg : pandas.core.df
        aggregate time series of the index
    agg_index_cont_tbl : pandas.core.df
        final/streamlined index contribution table for aggregation and analysis. This is what's written into the SQL table
    as_of_date : datetime
        current date
    unique_release_dts : list

            Unique release dates for the currency
    '''

    # Import dependencies into class namespcae
    import pyodbc
    import sqlalchemy
    import urllib
    import warnings
    import time
    import pandas as pd
    import numpy as np
    import datetime
    from matplotlib import pyplot as plt
    from matplotlib import dates as mdates
    import mysql.connector
    from mysql.connector.constants import ClientFlag
    import pymysql

    # Default arguments / settings for imported module
    plt.style.use('ggplot')

    # Define default class attributions - will be inherited by instances of Pulse
    unique_ccy_full = ['CHF','NZD','SEK','MXN','JPY','EUR','GBP','AUD','SGD','TRY','CNH','CAD','NOK','ZAR', 'USD'] # Add the list of supported currencies into class attribution
    default_sector_w = {'Survey': 0.2, 'Inflation': 0.2, 'Growth':0.2, 'Employment':0.2, 'Housing':0.2} # Default Sector weighting

    def __init__(self, conn_str:str, currency:str='USD'):

        '''Initailise Pulse instance attributes'''

        # SQL / Connection related attributes
        self.conn_str = conn_str # Update conn_str attribution for the instance
        self.conn_flag = 0 # Initial connection status: 0 = Not connected / 1 = Connected to SQL DB via ODBC

        # Table related attributes
        self.sql_tbl = [] # Empty list - placeholder for SQL pandas df for macro economic data
        self.weight_tbl = [] # Empty list - placeholder for indicator weight on release dates
        self.index_tbl = [] # Empty list - placeholder for indicator weight and percentile table
        self.index_ts_agg = [] #Empty list - placeholder for aggregate index time series
        self.agg_index_cont_tbl = [] # Empty list - placeholder for holding aggregate index score contribution by release date for all currencies

        # Date/dates released attributes
        self.as_of_date = self.datetime.date.today() # Default as of date is today for the new instance
        self.unique_release_dts = [] # Empty list - placeholder for a list of unquie release dates

        # Handle currency attribute
        if currency in self.unique_ccy_full: # Update currency attribution only if it's valid
            self.currency = currency
        else: self.warnings.warn('Input currency {} is NOT in the list of supported currencies. Fallback to default = USD'.format(currency))

    def retrieve_sql_tbl(self):
        '''Class method to retrieve the relevant table from SQL database. Requires conn attribute correctly configured. '''

        ## Test connection & raise exception message if fails
        try:
            self.conn = self.mysql.connector.connect(**self.conn_str) # Create PYODBC connection for the instance
            self.conn_flag = 1 # Update connection status
        except Exception as e:
            print('Error in establishing MySQL connection: \n' + e)
            return

        ## retrieve database table for the nominated currency
        try:
            self.sql_tbl = self.pd.read_sql("SELECT * FROM eco_data WHERE country='{}'".format(self.currency), self.conn)  # SQL statement to execute. May need to update if DB structure change
        except Exception as e:
            print('Error in reading SQL table for {} \n'.format(self.currency) + e)
            return

        ## Dropping True duplicates -> Any row that is identical to another on all variables is dropped
        self.sql_tbl.drop_duplicates(inplace = True)

        # PATCh: Deal with 'Busines Survey' vs 'Survey' which impacts weight_tbl calc. Mainly for for MXN / ZAR
        # if self.currency in ['MXN', 'ZAR']:
        #     self.sql_tbl['sector'] = self.sql_tbl['sector'].replace('Business Survey', 'Survey')

        ## Update instance attributes
        # Update as of date to the later of today and release date
        self.as_of_date = min(self.as_of_date, max(self.sql_tbl['release_date'].dt.date))
        self.unique_release_dts = self.sql_tbl['release_date'].sort_values(ascending=True).unique()
        self.unique_ccy_full = self.pd.read_sql("SELECT DISTINCT country FROM eco_data", self.conn)['country'].tolist() # Dynamically update unique_ccy_full list with what's in the DB

    def compute_weight(self, sector_w:dict=default_sector_w):
        '''Class method to compute contribution function for each release date. Requires sql_tbl attribute from pulse.retrieve_sql_tbl() method'''

        ## Some high level checks & swtiches
        if type(self.sql_tbl) == list: # List implies sql_tbl hasn't been read
            print('SQL table is empty in the current instance. Run retrieve_sql_tbl first')
            return
        elif not sector_w.keys() == self.default_sector_w.keys() or sum(sector_w.values()) != 1:
            print('Check sector weight dictionary input - Incorrect total weight or sectors')

        ## Calculation - start v0.01 wtih full calc, later consider the optionality of only updating a few dates?
        sector_w_map_lst = [] # Empty dlist for appending columns

        for release_date in self.unique_release_dts:

            # Generate lookback set, count number of unique data_name / BBG_code per Sector
            lookback_set = self.sql_tbl[self.sql_tbl['release_date']<= release_date]
            sector_freq = lookback_set.drop_duplicates(subset = ['data_name', 'bbg_code'])['sector'].value_counts().to_dict()
            sector_scale = sum([sector_w[key] for key in sector_freq.keys()]) # Total weighting of non-zero count sectors for scale

            # Compute a matrix with each row vector of the form (release_date, sector, indicator_weight scaled by total sector weight) for constructing Pandas df map
            sector_w_map_lst += [[release_date, key, (sector_w[key] / sector_scale) * (100 / sector_freq[key]) ] for key in sector_w.keys() & sector_freq]

        # Now convert mapping into a full pandas df map and merge
        sector_w_map = self.pd.DataFrame(sector_w_map_lst, columns=['release_date', 'sector', 'weight'])

        # Final step - merge by sector AND timestamp on release date, compute directional score
        self.weight_tbl = self.sql_tbl.merge(sector_w_map[['release_date', 'sector', 'weight']], how = 'left', on = ['sector', 'release_date'])
        self.weight_tbl['weight_dir'] = self.weight_tbl['weight'] * self.weight_tbl['score_direction']

    def compute_index(self, window_yr:float=2, pct_method:str='uniform'):

        ''' Compute index value for each indicator at each unique release date used as building blocks of Pulse. Optional input include window_yr (float) and method for percentile calculation.

        Note window_yr period is the same for quarterly and monthly releases - i.e. window_yr * 12 months and window_yr * 4 quarters. Therefore input 'window_yr' must in increments of .25 '''

        ## Some high level checks & swtiches
        if type(self.weight_tbl) == list: # List implies weight_tbl hasn't been computed
            print('Weight table is empty in the current instance. Run compute_weight first')
            return
        elif window_yr % 0.25 != 0:
            print('Optional window input must be a multiple of 0.25 to ensure whole monthly/quarterly window')
            return

        ## Set-up required objects
        weight_tbl = self.weight_tbl # Copy a working table to reduce number of self. calls
        release_date = self.pd.DataFrame(self.unique_release_dts, columns=['release_date']) # retrieve unique release dates from instance attribute, convert to DF
        unique_BBG_list = self.sql_tbl['bbg_code'].drop_duplicates() #Extract unique indicator list
        pctile_list = [] # Assign empty list for compiling required result in the form of a list

        for bbg_code in unique_BBG_list:
        # For each indicator - compute the percentile value at each new release_date using the designated window

            # Need to check whether we need to handle max_count
            if weight_tbl[weight_tbl['bbg_code'] == bbg_code]['freq'].unique() == 'Monthly':
                moving_window = 12 * window_yr  # unit = Months
            elif weight_tbl[weight_tbl['bbg_code'] == bbg_code]['freq'].unique()  == 'Quarterly':
                moving_window = 4 * window_yr # unit = Quarters
            else:
                print('Check frequency column content - must be either \'Monthly\' or \'Quarterly\'.')
                return

            # Grab required data for current unique BBG code
            # Sorting is done by first release_date asc, then effective_date asc
            indicator_tbl = weight_tbl[weight_tbl['bbg_code'] == bbg_code].sort_values(by=['release_date', 'effective_date'], ascending=[True, True])

            ## Compute Uniform Percentile - Add other methods later, maybe wrap within the class

            # Define short functions for rolling apply
            f_window_min = lambda x: self.pd.Series(x).min() # Compute min given input of Pandas series
            f_window_range = lambda x: (self.pd.Series(x).max() - self.pd.Series(x).min()) # Compute range given input of Pandas series

            # Compute rolling min, max and percentile
            indicator_tbl['min'] = indicator_tbl['value'].rolling(window=moving_window).apply(f_window_min)
            indicator_tbl['range'] = indicator_tbl['value'].rolling(window=moving_window).apply(f_window_range)
            indicator_tbl['percentile'] = 2* ((indicator_tbl['value'] - indicator_tbl['min']) / indicator_tbl['range'] - 0.5).round(2) # See documentation for uniform pctile calc

            # Compute indicator index value, merge and stack onto list
            # Duplicate by release_date still present in this step
            indicator_tbl['index_value'] = indicator_tbl['percentile'] * indicator_tbl['weight_dir'] # Percentile value x direction-adjusted weight
            # indicator_tbl['index_value'].round(decimals=2) # Round 2 d.p

            #indicator_out = release_date.merge(indicator_tbl, how='left', on='release_date').fillna(method='ffill')
            indicator_out = release_date.merge(indicator_tbl, how='left', on='release_date')
            indicator_out['release_flag'] = (~indicator_out['index_value'].isna()).map({True:1, False:0})
            indicator_out.fillna(method='ffill', inplace=True) #Now fill forward inplace

            # Remove Release_Date duplicates here
            '''
            We've used the full data set to compute so no longer need the duplicate release_date data points. Drop for contribution calculation.
            '''
            indicator_out.sort_values(by = ['release_date', 'effective_date'], ascending = [True, True], inplace=True)
            indicator_out.drop_duplicates(subset=['bbg_code', 'release_date'], keep='last', inplace=True)

            # Compute indicator contribution to Pulse
            '''
            First computing the diff, then backfill NaNs with 0. Finally correct for first release date.
            '''
            indicator_out['index_cont'] = indicator_out['index_value'].diff(periods=1)
            indicator_out['index_cont'].fillna(0, inplace=True)

            first_non_NaN = indicator_out['index_value'].first_valid_index()

            indicator_out['index_cont'][first_non_NaN] = indicator_out['index_value'][first_non_NaN]

            # To deal with the first release date score being removed. Won't affect the output release_dates since NA still present in other columns

            indicator_out.dropna(inplace=True) # Drop any NA in place - these are 'burn-in' periods where the window is <24month. Keeping rows where Index_Cont is NA (i.e. first available release date for each indicator)

            pctile_list += indicator_out.values.tolist() # final clean up, reset index to convert and append pctile_list obj

        # Finally creating the full dataframe. Note duplicate in release_date has already been removed in the loop by BBG_code
        self.index_tbl = self.pd.DataFrame(pctile_list, columns = indicator_out.columns).sort_values(by = ['release_date', 'effective_date'], ascending = [True, True])
        # self.index_tbl.drop_duplicates(subset=['bbg_code', 'release_date'], keep='last', inplace=True)

        # Final clean ups for index_tbl
        self.index_tbl.reset_index(inplace=True, drop=True) # reset index
        self.index_tbl.rename(columns={'value':'reading', 'index_value':'pulse_cont', 'index_cont':'pulse_cont_delta'}, inplace=True) # Quick rename for better reading

    def index_agg(self, plot:bool=True):

        ''' Compute aggregate country level Pulse index given index_tbl and plot time series '''

        ## Some high level checks & swtiches
        if type(self.index_tbl) == list: # List implies index_tbl hasn't been computed...
            print('Index table is empty in the current instance. Run compute_index first before aggregating')
            return

        ## First handle duplicate on release_date
        ''' First we sort by release_date, then effective_date, ascending. This puts duplicates by release_date in ascending order by effective_date. Then we drop_duplicates by keeping only the last duplicate across the same release_date'''

        index_tbl_noDup = self.index_tbl.sort_values(by = ['release_date', 'effective_date'], ascending = [True, True])
        index_tbl_noDup.drop_duplicates(subset=['bbg_code', 'release_date'], keep='last', inplace=True)

        ## We now set the now unique release_date as our index and aggregate by index
        index_tbl_noDup.set_index('release_date', inplace=True)
        self.index_ts_agg = index_tbl_noDup['pulse_cont'].groupby(level=0).sum()

        ## Generate a diagnostic line plot
        if plot:
            # Dynamically define a bunch of things...
            label = self.currency + ' Pulse Spot Level'
            xlabel = 'Date'
            ylabel = 'Index Level'
            title = self.currency + ' Aggregate Index'

            self.plt.plot_date(x = self.index_ts_agg.index, y = self.index_ts_agg.values, label = label, linewidth = 2, linestyle = '-', color = 'black', marker = None)

            # Generate plot
            self.plt.xlabel(xlabel)
            self.plt.ylabel(ylabel)
            self.plt.title(title)

            self.plt.legend()
            self.plt.tight_layout()
            self.plt.show()

            self.plt.gcf().autofmt_xdate() # Set time series x label

    def generate_idx(self, overwrite:bool=True, plot:bool=False):

        ''' Wrapper method for generating index for a given current currency. Essentially automates the process'''

        self.retrieve_sql_tbl() # retrieve econ data from sql table
        self.compute_weight() # Compute release_date weight by indicator
        self.compute_index() # Compute index
        self.index_agg(plot=plot) # Generate aggregate index time series / plot

        # Write to agg index table for the single currency
        if overwrite or (type(self.agg_index_cont_tbl) == list):
            self.agg_index_cont_tbl = self.index_tbl[['release_date', 'country', 'data_name', 'sector', 'reading', 'release_flag', 'pulse_cont', 'pulse_cont_delta']]
        else:
            new_tbl = self.index_tbl[['release_date', 'country', 'data_name', 'sector', 'reading', 'release_flag', 'pulse_cont', 'pulse_cont_delta']]
            self.agg_index_cont_tbl = self.pd.concat([self.agg_index_cont_tbl, new_tbl], ignore_index = True) # Update by concat

    def generate_idx_all_ccy(self, con_str:str, ssl_args:dict, table:str='pulse_score', plot:bool=False):

        ''' Wrapper method for computing index for all currencies for the purpose of writing/updating the sql table'''

        self.agg_index_cont_tbl = [] # Empty the entire table

        # Init a pulse_temp instance rather than updating current
        pulse_temp = Pulse(self.conn_str)

        for ccy in self.unique_ccy_full:
            start_t = self.time.time()
            pulse_temp.currency = ccy
            pulse_temp.generate_idx(overwrite=False, plot=plot)
            print('Pulse index contribution table computed for', ccy, 'computation time:', (self.time.time() - start_t).__round__(2))

        # Save
        self.agg_index_cont_tbl = pulse_temp.agg_index_cont_tbl

        # Write SQL
        self.write_sql_tbl(con_str=con_str, ssl_args=ssl_args, table=table)
        print('Aggregate Pulse index contribution table written to SQL!') # Done!

    def write_sql_tbl(self, con_str:str, ssl_args:dict, table:str='pulse_score'):

        #con_str = "mysql+pymysql://root:52alpha@34.72.43.169/macro_dash_db"
        #ssl_args = {'ssl':{'cert': r'C:\git\google_cloud_db\client-cert.pem',
        #                    'key': r'C:\git\google_cloud_db\client-key.pem',
        #                    'ca':r'C:\git\google_cloud_db\server-ca.pem',
        #                    'check_hostname':False}}

        ''' Generate required sql table content and write to sql table via ODBC connection'''

        # Check if agg_index_tbl is present
        if type(self.agg_index_cont_tbl) == list:
            print('Index contribution table is empty. Run calculation first.')
            return

        ## Test connection & raise exception message if fails
        if self.conn_flag == 0:
            try:
                self.conn = self.mysql.connector.connect(**self.conn_str) # Create MySQL connection for the instance
                self.conn_flag = 1 # Update connection status
            except Exception as e:
                print('Error in establishing MYSQL connection: \n' + e)
                return

        # Use URLIB to convert delimeters for sql alchmey engine
        # params = self.urllib.parse.quote_plus(**self.conn_str)
        engine = self.sqlalchemy.create_engine(con_str,connect_args=ssl_args)
        engine.connect() # Added to catch engine connection error
        print(table) # Check table has default value or input

        # Write to SQL table
        self.agg_index_cont_tbl.to_sql(name=table, con=engine, if_exists='replace', index=False)
