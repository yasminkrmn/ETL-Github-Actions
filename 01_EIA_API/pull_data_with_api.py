import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import src.eia_api as api

pd.set_option('display.max_columns', None)


# ===========================
# 1. SENDING A SIMPLE GET REQUEST
# ===========================

# ===========================
# 1.1 SETTING UP A GET REQUEST
# ===========================


api_key = os.getenv('EIA_API_Key')
api_path = "electricity/rto/region-sub-ba-data/data/"
frequency = "hourly"
facets = {
    "parent": "CISO",
    "subba": "PGAE"
}

df1 = api.eia_get(
    api_key = api_key,
    api_path = api_path,
    frequency = frequency,
    facets = facets
)

df1.url
# 'https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/?data[]=value&facets[parent][]=CISO&facets[subba][]=PGAE&frequency=hourly&api_key='

df1.parameters
# {'api_path': 'electricity/rto/region-sub-ba-data/data/',
#  'data': 'value',
#  'facets': {'parent': 'CISO', 'subba': 'PGAE'},
#  'start': None,
#  'end': None,
#  'length': None,
#  'offset': None,
#  'frequency': 'hourly'}

type(df1.data)
# pandas.core.frame.DataFrame
df1.data.dtypes

# period         datetime64[ns]
# subba                  object
# subba-name             object
# parent                 object
# parent-name            object
# value                   int64
# value-units            object
# dtype: object


# ===========================
# 2. API LIMITATION
# ===========================

# Let's plot the series:

plt.figure(figsize=(20, 10))
plt.plot(df1.data["period"], df1.data["value"], color='b')
plt.xlabel('Period')
plt.ylabel('Value')
plt.title('Time Series Plot of Value Over Time')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# The start and end arguments enable us to set a time range to the GET request.
# For example, let's pull data betweem January 1st, 2024 and February 24th, 2024:

start = datetime.datetime(2024, 1, 1, 1)
end = datetime.datetime(2024, 2, 24, 23)

df2 = api.eia_get(
    api_key = api_key,
    api_path = api_path,
    frequency = frequency,
    facets = facets,
    start = start,
    end = end
)
df2.data.columns

# (['period', 'subba', 'subba-name', 'parent', 'parent-name', 'value',
#        'value-units'],
#       dtype='object')


plt.figure(figsize=(20, 10))
plt.plot(df2.data["period"], df2.data["value"], color='b')
plt.xlabel('Period')
plt.ylabel('Value')
plt.title('Time Series Plot of Value Over Time')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# =================================
# 3. HANDLING A LARGE DATA REQUEST
# =================================
# When we have to pull a series with a number of observations that exceed the API limitation of 5000 observations
# per call, use the eia_backfill function.
# The function splits the request into multiple small requests, where the offset argument defines the size of each request.
# It is recommended not to use an offset larger than 2500 observations.
# For example, let's pull data since July 1st, 2018:

start = datetime.datetime(2018, 7, 1, 8)
end = datetime.datetime(2024, 2, 24, 23)
offset = 2250

df3 = api.eia_backfill(
  start = start,
  end = end,
  offset = offset,
  api_path= api_path,
  api_key = api_key,
  facets = facets)


df3.data

# Out[13]:
#                   period subba  ...  value    value-units
# 0    2023-07-30 15:00:00  PGAE  ...  10355  megawatthours
# 1    2023-07-30 16:00:00  PGAE  ...  10595  megawatthours
# 2    2023-07-30 17:00:00  PGAE  ...  10535  megawatthours
# 3    2023-07-30 18:00:00  PGAE  ...  10157  megawatthours
# 4    2023-07-30 19:00:00  PGAE  ...   9982  megawatthours
#                   ...   ...  ...    ...            ...
# 4995 2024-02-23 20:00:00  PGAE  ...  10113  megawatthours
# 4996 2024-02-23 21:00:00  PGAE  ...   9365  megawatthours
# 4997 2024-02-23 22:00:00  PGAE  ...   8969  megawatthours
# 4998 2024-02-23 23:00:00  PGAE  ...   8656  megawatthours
# 4999 2024-02-24 00:00:00  PGAE  ...   8777  megawatthours
# [5000 rows x 7 columns]













