import datetime
from utils import CONNECTOR
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()

from_datetime_str = '2022-2-1'
to_datetime_str = '2022-2-28 23:00'

transformer_33kv_max_query_str = f"""
SELECT s.id AS ss_id, MW.date_time, max(abs(MW.value)) as MW
-- tt.voltage_level, se.is_transformer_low
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN transformer AS t ON se.transformer_id = t.id
JOIN transformer_type AS tt ON tt.id = t.type_id
JOIN substation AS s ON se.substation_id = s.id
WHERE se.is_transformer_low = 1
AND (tt.id = 1 OR tt.id = 8)
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY s.id
"""

generation_33kv_max_str = f"""
SELECT s.id AS ss_id, MW.date_time, max(abs(MW.value)) AS MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id
-- JOIN zone AS z ON z.id = s.zone
-- JOIN gmd ON gmd.id = s.gmd
WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY s.id
"""

ss_query_str = """
SELECT s.id AS ss_id, s.name AS ss, zone.name AS zone, gmd.name AS gmd
FROM
substation AS s
JOIN zone ON s.zone = zone.id
JOIN gmd ON s.gmd = gmd.id
"""

transformer_33kv_max_df = pd.read_sql_query(transformer_33kv_max_query_str, CONNECTOR, index_col='ss_id')
generation_33kv_max_df = pd.read_sql_query(generation_33kv_max_str, CONNECTOR, index_col='ss_id')
ss_df1 = pd.read_sql_query(ss_query_str, CONNECTOR, index_col='ss_id')


# def is_33_present(_str):
#     return '/33' in _str
#
#
# ss_df = ss_df1[ss_df1.ss.map(is_33_present)]
ss_df = ss_df1

for i in generation_33kv_max_df.index:
    transformer_33kv_max_df.loc[i, 'MW'] += generation_33kv_max_df.loc[i, 'MW']

# ss_max_mw_df = ss_df.join(transformer_33kv_max_df).sort_values(by=['zone', 'gmd'], ignore_index=True)
ss_max_mw_df = ss_df.join(transformer_33kv_max_df).sort_values(by=['ss'], ignore_index=True, ascending=False)
ss_max_mw_df.to_excel('max_mw.xlsx')
print(f'time elapsed = {datetime.datetime.now() - t1}')
a = 5

ss_max_mw_df_value_only = ss_max_mw_df.loc[:, ['ss', 'MW']]
# ss_max_mw_df_value_only = ss_max_mw_df_value_only.set_index('ss')
import plotly.express as px
import plotly.io as pio
pio.renderers.default = "browser"
# df = px.data.tips()
fig = px.bar(ss_max_mw_df_value_only, y="ss", x='MW', width=1800, height=2000,
             text_auto='3s'
             )
fig.update_layout(
        title=f"Substation Max Load: From {str(pd.to_datetime(from_datetime_str))} to "
              f"{str(pd.to_datetime(to_datetime_str))}",
        xaxis_title="Max Load (MW)",
        yaxis_title="Substation",
        legend_title="Legend Title",
        font=dict(
            family="Times New Roman, monospace",
            size=15,
            color="RebeccaPurple"
        ),
)
fig.update_traces(textfont_size=50, textangle=0, textposition="outside", cliponaxis=False)
# fig.show()
fig.write_html('titleString' + 'plot.html', auto_open=True)
a = 5