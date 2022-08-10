"""
TODO related
* switch pull time to uniform time for all scrapers (maybe just add col)
* switch to simpler timestamp
* switch to timestamp type in schema
    * then can get day etc from that
* get mapping from ID to zip
    * how to do this fast?
"""

"""
High level

* get current date (or specified monday sunday closest to run date?)
* get pull that happened closest to that date (presumably there was a pull on that day)
* filter to that pull and a week prior to that pull 
* get day of the week for date col

* need to get sep cols for last week and this week piv out and joined
* join id, zip 

* agg by zip calc metrics

"""



import pandas as pd
import matplotlib.pyplot as plt
import folium
import numpy as np
import seaborn as sns
import os
import fastparquet
import warnings
import geopy
from geopy.point import Point
import time
from pandas.core.common import SettingWithCopyWarning
import plotly
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, date
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Load in occupancy data
occ_data = pd.read_parquet('C:/Users/mattg/Desktop/Hobbies/airbnb_reports/bucket_data/occupancy')

# Grab next month of data from last weeks pull vs this weeks pull to see how occupancy has changed week over week
fwd_looking_days = 28
last_day_from = pd.to_datetime('05/31/2022')
pull_time_current = pd.to_datetime('05/05/2022') # Would be set as date for "today" in the future
fwd_looking_date_current = pull_time_current + timedelta(days=fwd_looking_days)
pull_time_old = last_day_from - timedelta(days=7)
fwd_looking_date_old = pull_time_old + timedelta(days=fwd_looking_days)
occ_data_last30_current = occ_data[(occ_data['date'] > str(pull_time_current)) & (occ_data['date'] <= str(fwd_looking_date_current))]
occ_data_last30_old = occ_data[(occ_data['date'] > str(pull_time_old)) & (occ_data['date'] <= str(fwd_looking_date_old))]
"""
Justin Understanding

Here we have two sets - one is last week looking fwd 1 mo, second is "today" looking fwd 1 mo

TODO
* we will probably want this to filter to exactly 1 pull date for "today" and "a week ago" 
  otherwise actually have a week of occupancy pulls in the set when we just want a fwd looking
  snapshot 
"""
print(occ_data_last30_current.shape)
print(occ_data_last30_old.shape)
occ_data_last30_current['date'] = pd.to_datetime(occ_data_last30_current['date'])
occ_data_last30_old['date'] = pd.to_datetime(occ_data_last30_old['date'])
occ_data_last30_current['day_of_week'] = occ_data_last30_current['date'].dt.dayofweek
occ_data_last30_old['day_of_week'] = occ_data_last30_old['date'].dt.dayofweek
"""
we then are getting averages for each day of the week for each ID in that fwd looking month
we are doing mean on available - so should be sum() over count()? thats what we want I think 
    that would be % available basically for that day and ID 
    so 4 weeks of data for an ID we get average of availability for that day for the id 0, .25, .5 .75 or 1
"""
occ_data_previous_pull_avgs = occ_data_last30_old.groupby(['id', 'day_of_week']).mean().reset_index()
print(occ_data_previous_pull_avgs.shape)
occ_data_current_pull_avgs = occ_data_last30_current.groupby(['id', 'day_of_week']).mean().reset_index()
print(occ_data_current_pull_avgs.shape)
"""
join prev to this week df 
TODO why not join on day of week also
"""
occ_trend = occ_data_previous_pull_avgs.merge(occ_data_current_pull_avgs, how='inner', on='id')
print(occ_trend.shape)
occ_trend.to_csv('occ_trend_miami_for_table_weekly_july30.csv')

# Add calculated columns finding changes week over week
occ_trend['available_delta_pct'] = (occ_trend['available_y'] - occ_trend['available_x']) * 100
occ_trend['available_for_checkin_delta_pct'] = (occ_trend['available_for_checkin_y'] - occ_trend['available_for_checkin_x']) * 100
occ_trend['available_for_checkout_delta_pct'] = (occ_trend['available_for_checkout_y'] - occ_trend['available_for_checkout_x']) * 100
occ_trend['bookable_delta_pct'] = (occ_trend['bookable_y'] - occ_trend['bookable_x']) * 100

# Read in real estate data for quick mapping of location attributes
"""
TODO will need to add this stuff in post-processing probably to the listing table

"""
data_w_listing_loc = pd.read_csv('post_mort_and_roi_calcs_june19.csv')
data_w_listing_loc = data_w_listing_loc[['id','zipcode','City','Metro','CountyName','State']]

# Merge Occupancy data with location data
occ_trend['id'] = occ_trend['id'].astype('float64')
occ_trend_wloc = occ_trend.merge(data_w_listing_loc, how='inner', on='id')

## Groupbys for tables
# Occupancy Trends week over week by Zipcode
"""
we then end up with aggregated percentages by zip
"""
occ_trends_by_zip = occ_trend_wloc.groupby('zipcode').mean(['available_delta_pct','available_for_checkin_delta_pct','available_for_checkout_delta_pct','bookable_delta_pct']).reset_index()

# Occupancy Trends week over week by Zipcode and Day of Week
occ_trends_by_zip_dow = occ_trend_wloc.pivot_table(index=['zipcode','day_of_week_x'], values='available_delta_pct', aggfunc='mean')
