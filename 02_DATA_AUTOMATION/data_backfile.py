################################################
# EIA API - DATA BACKFILL
################################################

# The goal of this doc is to execute an initial data pull of the hourly demand for California balancing
# authority subregion (CISO). This includes the following four independent system operators:

# - Pacific Gas and Electric (PGAE)
# - Southern California Edison (SCE)
# - San Diego Gas and Electric (SDGE)
# - Valley Electric Association (VEA)

# The data backfill process includes the following steps:
# - Setting parameters and pulling the data
# - Data quality checks
# - Saving the data and creating a log file

# ===========================
# 1. LOAD LIBRARIES
# ===========================

import src.eia_api as api
import src.eia_data as Eia_data
import pandas as pd
import numpy as np
import requests
import json
import os
import datetime
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)


# ===========================
# 2. SETTING PARAMETERS
# ===========================
eia_api_key = os.getenv('API_Key')

raw_json = open("metadata/series.json")
meta_json = json.load(raw_json)
series = pd.DataFrame(meta_json["series"])
api_path = meta_json["api_path"]


facets_template = {
  "parent" : None,
  "subba" : None
}

start = datetime.datetime(2018, 7, 1, 8)
end = datetime.datetime(2024, 2, 18, 1)
offset = 2250

# ===========================
# 3. PULLING DATA
# ===========================

metadata = api.eia_metadata(api_key = eia_api_key, api_path = api_path)
print(metadata.meta.keys())
print(metadata.meta["startPeriod"])
print(metadata.meta["endPeriod"])


for i in series.index:
  facets = facets_template
  facets["parent"] = series.at[i, "parent_id"]
  facets["subba"] = series.at[i, "subba_id"]
  print(facets)
  temp = api.eia_backfill(api_key = eia_api_key,
        api_path = api_path+ "data",
        facets = facets,
        start = start,
        end = end,
        offset = offset)
  ts_obj = pd.DataFrame(np.arange(start = start, stop = end + datetime.timedelta(hours = 1),
                                  step = datetime.timedelta(hours = 1)).astype(datetime.datetime),
                        columns=["index"])
  ts_obj  = ts_obj.merge(temp.data, left_on = "index", right_on = "period", how="left")
  ts_obj.drop("period", axis = 1, inplace= True)
  ts_obj = ts_obj.rename(columns= {"index": "period"})

  meta_temp = Eia_data.create_metadata(data = ts_obj, start = start, end = end, type = "backfill")
  meta_temp["index"] = 1
  meta_df = pd.DataFrame([meta_temp])

  if i == series.index.start:
    data = ts_obj
    meta = meta_df
  else:
    data = data._append(ts_obj)
    meta = meta._append(meta_df)

# ===========================
# 4. DATA QUALITY CHECKS
# ===========================
print(meta)
#    index parent subba                             time               start  \
# 0      1   CISO  PGAE 2024-09-30 00:50:36.215848+00:00 2018-07-01 08:00:00
# 0      1   CISO   SCE 2024-09-30 00:50:37.623904+00:00 2018-07-01 08:00:00
# 0      1   CISO  SDGE 2024-09-30 00:50:39.114743+00:00 2018-07-01 08:00:00
# 0      1   CISO   VEA 2024-09-30 00:50:40.563749+00:00 2018-07-01 08:00:00
#                   end           start_act             end_act  start_match  \
# 0 2024-02-18 01:00:00 2018-07-01 08:00:00 2024-02-18 01:00:00         True
# 0 2024-02-18 01:00:00 2018-07-01 08:00:00 2024-02-18 01:00:00         True
# 0 2024-02-18 01:00:00 2018-07-01 08:00:00 2024-02-18 01:00:00         True
# 0 2024-02-18 01:00:00 2018-07-01 08:00:00 2024-02-18 01:00:00         True
#    end_match  n_obs     na      type  update  success  \
# 0       True  49386  44386  backfill   False    False
# 0       True  49386  44386  backfill   False    False
# 0       True  49386  44386  backfill   False    False
# 0       True  49386  44386  backfill   False    False
#                       comments
# 0  Missing values were found;
# 0  Missing values were found;
# 0  Missing values were found;
# 0  Missing values were found;

# ===========================
# 5. SAVING DATA
# ===========================

meta_path = "metadata/ciso_log.csv"
data_path = "csv/ciso_data.csv"
d = Eia_data.append_data(data_path = data_path, new_data = data, init = True, save = True)

# ===========================
# 6. SAVING META-DATA AS LOG CSV FILE
# ===========================
meta["success"] = True
meta["update"] = True
m = Eia_data.append_metadata(meta_path = "metadata/ciso_log.csv", meta = meta, save = True, init = True)
print(m)

# ===========================
# 6. PLOTTING THE SERIES
# ===========================

data_sorted = data.sort_values(by=["subba", "period"])

plt.figure(figsize=(20, 10))

# Iterate through each unique group in 'subba' and plot them
for label, group in data_sorted.groupby('subba'):
    plt.plot(group['period'], group['value'], label=label)  # Plot each group with a different color

plt.xlabel('Period')
plt.ylabel('Value')
plt.title('Time Series Plot of Value Over Time by Subba')
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()



