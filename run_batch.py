import datetime
import sys
import pandas as pd

udf_path= r"C:\git\pulse_daily_batch_container"  # batch job container directory
sys.path.append(udf_path)

from update_prices import update_prices
from update_econ import scrap_eco_data
from update_score import update_score

current_date=datetime.date.today()
run_date=current_date-pd.tseries.offsets.BDay(1)

# print(run_date)
update_prices(current_date)
scrap_eco_data(run_date.strftime("%m/%d/%Y"),run_date.strftime("%m/%d/%Y"))
update_score()
