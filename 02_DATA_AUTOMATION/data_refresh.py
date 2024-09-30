################################################
# EIA API - DATA REFRESH
################################################


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
import great_tables as gt

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
offset = 2250

meta_path = "metadata/ciso_log.csv"
data_path = "csv/ciso_data.csv"

# api_metadata = api.eia_metadata(api_key = eia_api_key, api_path = api_path)
# print(api_metadata.meta["endPeriod"])
# end = pd.to_datetime(api_metadata.meta["endPeriod"])
# print(end)

# ===========================
# 3. PULLING DATA
# ===========================

meta_obj = Eia_data.get_metadata(api_key = eia_api_key, api_path = api_path, meta_path = meta_path, series = series)
gt.GT(meta_obj.request_meta,)
# GT(_tbl_data=      parent subba             end_act       request_start  \
# index
# 0       CISO  PGAE 2024-02-18 01:00:00 2024-02-18 02:00:00
# 1       CISO   SCE 2024-02-18 01:00:00 2024-02-18 02:00:00
# 2       CISO  SDGE 2024-02-18 01:00:00 2024-02-18 02:00:00
# 3       CISO   VEA 2024-02-18 01:00:00 2024-02-18 02:00:00
#                       end  updates_available
# index
# 0     2024-09-29 07:00:00               True
# 1     2024-09-29 07:00:00               True
# 2     2024-09-29 07:00:00               True
# 3     2024-09-29 07:00:00               True

# ===========================
# 3. REFRESHING DATA
# ===========================

m = meta_obj.request_meta
index = meta_obj.last_index + 1
data = None

for i in m.index:

    facets = facets_template
    facets["parent"] = m.at[i, "parent"]
    facets["subba"] = m.at[i, "subba"]
    start = m.at[i, "request_start"]
    end = m.at[i, "end"]

    print(facets)
    if m.at[i, "updates_available"]:
        temp = api.eia_backfill(api_key=eia_api_key,
                                api_path=api_path + "data",
                                facets=facets,
                                start=start.to_pydatetime(),
                                end=end.to_pydatetime(),
                                offset=offset)

        ts_obj = pd.DataFrame(
            np.arange(start=start, stop=end + datetime.timedelta(hours=1), step=datetime.timedelta(hours=1)).astype(
                datetime.datetime), columns=["index"])
        ts_obj = ts_obj.merge(temp.data, left_on="index", right_on="period", how="left")
        ts_obj.drop("period", axis=1, inplace=True)
        ts_obj = ts_obj.rename(columns={"index": "period"})
    else:
        ts_obj = None
        print("No new data is available")

    meta_temp = Eia_data.create_metadata(data=ts_obj, start=start, end=end, type="refresh")

    if ts_obj is None:
        meta_temp["parent"] = m.at[i, "parent"]
        meta_temp["subba"] = m.at[i, "subba"]

    if meta_temp["success"]:
        print("Append the new data")
        d = Eia_data.append_data(data_path=data_path, new_data=ts_obj, save=True)
        meta_temp["update"] = True
    else:
        meta_temp["update"] = False
        meta_temp["comments"] = meta_temp["comments"] + "The data refresh failed, please check the log; "

    meta_df = pd.DataFrame([meta_temp])

    if data is None:
        data = ts_obj
    else:
        data = data._append(ts_obj)

    if i == series.index.start:
        meta_new = meta_df
    else:
        meta_new = meta_new._append(meta_df)

gt.GT(meta_new,rowname_col = "index")

meta_updated = Eia_data.append_metadata(meta_path = meta_path,
                                        meta = meta_new,
                                        save = True,
                                        init = False)

# ===========================
# 4. PLOTTING DATA
# ===========================
if data is not None:
    # Sort the data by 'subba' and 'period'
    data_sorted = data.sort_values(by=["subba", "period"])

    # Create a color map for different groups
    plt.figure(figsize=(20, 10))

    # Iterate through each group in 'subba' and plot them with different colors
    for label, group in data_sorted.groupby('subba'):
        plt.plot(group['period'], group['value'], label=label)  # Plot each group

    plt.xlabel('Period')
    plt.ylabel('Value')
    plt.title('Time Series Plot of Value Over Time by Subba')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

else:
    print("No new data is available")

full_data = pd.read_csv(data_path)
full_data.head()
full_data["period"] = pd.to_datetime(full_data["period"])

# Create a line plot with matplotlib
plt.figure(figsize=(10, 6))

for label, df in full_data.groupby('subba'):
    plt.plot(df['period'], df['value'], label=label)

plt.xlabel('Period')
plt.ylabel('Value')
plt.title('Line Plot of Value Over Time by Subba')
plt.legend()
plt.show()



