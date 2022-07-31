import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def ss_max_load(from_datetime_str, to_datetime_str, html_path, excel_path=r'ss_max_load_mw.xlsx', max_thresh=350):
    transformer_33kv_max_query_str = f"""
    -- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
    SELECT s.id AS ss_id, MW.date_time, sum(abs(MW.value)) as MW
    -- tt.voltage_level, se.is_transformer_low
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN transformer AS t ON se.transformer_id = t.id
    JOIN transformer_type AS tt ON tt.id = t.type_id
    JOIN substation AS s ON se.substation_id = s.id
    WHERE se.is_transformer_low = 1
    AND (tt.id = 1 OR tt.id = 7 OR tt.id = 8)
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, ss_id
    """

    generation_33kv_max_str = f"""
    SELECT s.id AS ss_id, MW.date_time, sum(abs(MW.value)) AS MW
    -- f.is_generation, se.is_feeder
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN feeder AS f ON se.feeder_id = f.id
    JOIN substation AS s ON se.substation_id = s.id
    -- JOIN zone AS z ON z.id = s.zone
    -- JOIN gmd ON gmd.id = s.gmd
    WHERE f.is_generation = 1
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, ss_id
    """

    ss_query_str = """
    SELECT s.id AS ss_id, s.name AS ss
    FROM
    substation AS s
    """

    transformer_33kv_mw_df = pd.read_sql_query(transformer_33kv_max_query_str, CONNECTOR, index_col=['date_time', 'ss_id'])
    generation_33kv_mw_df = pd.read_sql_query(generation_33kv_max_str, CONNECTOR, index_col=['date_time', 'ss_id'])
    ss_df = pd.read_sql_query(ss_query_str, CONNECTOR, index_col='ss_id')

    # def is_33_present(_str):
    #     return '/33' in _str
    #
    #
    # ss_df = ss_df1[ss_df1.ss.map(is_33_present)]
    # ss_df = ss_df1

    for i in generation_33kv_mw_df.index:
        if i in transformer_33kv_mw_df.index:
            t_val = transformer_33kv_mw_df.loc[i, 'MW']
            gen_val = generation_33kv_mw_df.loc[i, 'MW']
            transformer_33kv_mw_df.loc[i, 'MW'] = t_val + gen_val

    # for i in transformer_33kv_mw_df.index:
    #     transformer_33kv_mw_df.loc[i, 'date_time'] = i[0]
    #     transformer_33kv_mw_df.loc[i, 'ss_id'] = i[1]

    ss_mw_df = transformer_33kv_mw_df.reset_index()
    # ss_mw_df = ss_mw_df.set_index('ss_id')
    idx = ss_mw_df.groupby(['ss_id'])['MW'].transform(max) == ss_mw_df['MW']
    # ss_max_mw_df1 = ss_mw_df.groupby(['ss_id'], sort=False).max()
    ss_max_mw_df1 = ss_mw_df[idx]
    ss_max_mw_df = ss_max_mw_df1.set_index('ss_id')
    a = 4
    # ss_max_mw_df = ss_df.join(transformer_33kv_max_df).sort_values(by=['zone', 'gmd'], ignore_index=True)
    ss_max_mw_df1 = ss_max_mw_df[~ss_max_mw_df.index.duplicated(keep='first')]
    ss_max_mw_df = ss_df.join(ss_max_mw_df1).sort_values(by=['ss'], ignore_index=False, ascending=True)
    # ss_max_mw_df1 = ss_df.join(ss_max_mw_df).sort_values(by=['ss'], ignore_index=True, ascending=True)

    """Remove garbage max entry"""
    for i in ss_max_mw_df.index:
        if ss_max_mw_df.loc[i, 'MW'] > max_thresh:
            ss_max_mw_df.loc[i, 'MW'] = pd.NA

    ss_max_mw_df1 = ss_max_mw_df.dropna(how='any', axis=0)
    ss_max_mw_df = ss_max_mw_df1.loc[:, ['ss', 'MW', 'date_time']].reset_index(drop=True)
    ss_max_mw_df.columns = ['Substation', 'Max Load Served (MW)', 'date_time']
    ss_max_mw_df.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'Excel written in {excel_path}')
    a = 5

    """HTML Conversion"""
    # ss_max_mw_df = ss_max_mw_df1.iloc[:, ::-1]
    # ss_max_mw_df_value_only = ss_max_mw_df1.loc[:, ['ss', 'MW', 'date_time']]
    # ss_max_mw_df_value_only = ss_max_mw_df_value_only.set_index('ss')
    import plotly.express as px
    import plotly.io as pio
    pio.renderers.default = "browser"
    # df = px.data.tips()
    fig = px.bar(ss_max_mw_df, y="Substation", x='Max Load Served (MW)', width=1700, height=3200,
                 text_auto=f'.3s', orientation='h', hover_data=['date_time']
                 )
    fig.update_layout(
        title=f"Substation Max Load: From {str(pd.to_datetime(from_datetime_str))} to "
              f"{str(pd.to_datetime(to_datetime_str))}",
        xaxis_title="Max Load (MW)",
        yaxis_title="Substation",
        legend_title="Legend Title",
        # font=dict(
        #     family="Times New Roman, monospace",
        #     size=15,
        #     color="RebeccaPurple"
        # ),
    )
    fig.update_traces(textfont_size=50, textangle=0, textposition="outside", cliponaxis=False)
    # fig.show()
    fig_html = fig.write_html(html_path, auto_open=False)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'HTML written in {html_path}')
    return ss_max_mw_df1


df = ss_max_load(from_datetime_str='2022-5-01 00:00', to_datetime_str='2022-5-31 23:00',
                 html_path='t.html', excel_path='max_mw.xlsx')
