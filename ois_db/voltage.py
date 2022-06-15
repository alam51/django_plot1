import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def ss_max_min_voltage(from_datetime_str, to_datetime_str, html_path, excel_path=r'ss_max_load_mw.xlsx'):
    max_voltage_query_str = f"""
    -- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
    SELECT se.substation_id AS 'ss_id', MAX(v.value) as 'max_kv', v.date_time as 'max_dt'
    FROM bus AS b
    JOIN substation_equipment AS se ON se.bus_id = b.id
    JOIN voltage AS v ON v.sub_equip_id = se.id
    WHERE v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    AND v.value < 500
    GROUP BY se.substation_id
    """

    min_voltage_min_query_str = f"""
    SELECT se.substation_id AS 'ss_id', MIN(v.value) as 'min_kv', v.date_time as 'min_dt', b.bus_voltage 
    FROM bus AS b
    JOIN substation_equipment AS se ON se.bus_id = b.id
    JOIN voltage AS v ON v.sub_equip_id = se.id
    WHERE v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    AND v.value > 50
    GROUP BY se.substation_id
    """

    ss_query_str = """
    SELECT s.id AS ss_id, s.name AS ss
    FROM
    substation AS s
    """

    equipment_voltage_query_str = """
    SELECT id, equipment_voltage.name as 'base_kv'  
    from equipment_voltage 
    """

    max_voltage_df = pd.read_sql_query(max_voltage_query_str, CONNECTOR, index_col=['ss_id'])
    min_voltage_df = pd.read_sql_query(min_voltage_min_query_str, CONNECTOR, index_col=['ss_id'])
    # generation_33kv_mw_df = pd.read_sql_query(generation_33kv_max_str, CONNECTOR, index_col=['date_time', 'ss_id'])
    ss_df = pd.read_sql_query(ss_query_str, CONNECTOR, index_col=['ss_id'])
    equipment_voltage_df = pd.read_sql_query(equipment_voltage_query_str, CONNECTOR)
    op_df1 = ss_df.join([max_voltage_df, min_voltage_df])
    op_df = op_df1.join(equipment_voltage_df, on='bus_voltage')
    op_df.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    # # ss_max_mw_df = ss_max_mw_df1.iloc[:, ::-1]
    # # ss_max_mw_df_value_only = ss_max_mw_df1.loc[:, ['ss', 'MW', 'date_time']]
    # # ss_max_mw_df_value_only = ss_max_mw_df_value_only.set_index('ss')
    # import plotly.express as px
    # import plotly.io as pio
    # pio.renderers.default = "browser"
    # # df = px.data.tips()
    # fig = px.bar(ss_max_mw_df, y="Substation", x='Max Load Served (MW)', width=1700, height=3200,
    #              text_auto=f'.3s', orientation='h', hover_data=['date_time']
    #              )
    # fig.update_layout(
    #     title=f"Substation Max Load: From {str(pd.to_datetime(from_datetime_str))} to "
    #           f"{str(pd.to_datetime(to_datetime_str))}",
    #     xaxis_title="Max Load (MW)",
    #     yaxis_title="Substation",
    #     legend_title="Legend Title",
    #     # font=dict(
    #     #     family="Times New Roman, monospace",
    #     #     size=15,
    #     #     color="RebeccaPurple"
    #     # ),
    # )
    # fig.update_traces(textfont_size=50, textangle=0, textposition="outside", cliponaxis=False)
    # # fig.show()
    # fig_html = fig.write_html(html_path, auto_open=False)
    # print(f'time elapsed = {datetime.datetime.now() - t1}')
    # print(f'HTML written in {html_path}')
    return op_df


df = ss_max_min_voltage(from_datetime_str='2022-4-1 00:00', to_datetime_str='2022-4-30 23:00',
                 html_path='t.html', excel_path='max_mw.xlsx')
