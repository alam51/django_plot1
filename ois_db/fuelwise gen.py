from utils import CONNECTOR_LDD
import pandas as pd

from_datetime = '2022-9-19 00:00'
to_datetime = '2022-10-4 23:00'
query_str = f"""
SELECT mw.date_time, g.grid_name, ps.fuel_id, f.name AS fuel_name, SUM(mw.value) as gen_mw
FROM mega_watt AS mw
JOIN generation_unit AS g ON g.id = mw.generation_unit_id
JOIN power_station AS ps ON g.power_station_id = ps.id
JOIN fuel AS f ON ps.fuel_id = f.id
WHERE (mw.date_time BETWEEN '{from_datetime}' AND '{to_datetime}')
-- AND g.grid_name = 'Eastern'
GROUP BY mw.date_time, g.grid_name, ps.fuel_id
"""

df = pd.read_sql_query(query_str, CONNECTOR_LDD, index_col='date_time')
# # a = pd.to_datetime(df['date_time'])
df['date'] = df.index.date
df['time'] = df.index.time
a = 4

# date_range = pd.date_range(start=from_datetime, end=to_datetime, freq='1D')
# for date in date_range:
#     current_df = df.loc[date: date + pd.to_timedelta('1D')]
#     # east_df = current_df[current_df.grid_name == 'Eastern']
#     # east_df_processed = east_df.pivot(index=['date', 'grid_name', 'fuel_name'], columns=['time'], values=['gen_mw'])
#     # east_df_processed.to_excel('gen.xlsx')
#
#     current_df_processed = current_df.pivot(index=['date', 'grid_name', 'fuel_name'], columns=['time'], values=['gen_mw'])
#     current_df_processed.to_excel('gen.xlsx')

df_processed = df.pivot(index=['date', 'grid_name', 'fuel_name'], columns=['time'], values=['gen_mw'])
df_processed.to_excel('gen.xlsx')
v = 1
