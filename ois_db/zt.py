import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def ss_max_min_voltage(from_datetime_str, to_datetime_str, excel_path=r'ss_max_load_mw.xlsx'):
    max_zt_query_str = f"""
    SELECT zone.name AS 'zone' , T_zt.zt AS 'MAX Zone Total (MW)', T_zt.date_time FROM 
    (
    SELECT T_zt.zone, MAX(T_zt.zt) AS zt_max FROM 
    (
    SELECT T_tr.zone, T_tr.date_time, tr_MW+IFNULL(T_gen.gen_MW,0) AS zt
    FROM
    (
    SELECT s.zone, MW.date_time, sum(MW.value) as tr_MW
     -- tt.voltage_level, se.is_transformer_low
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN transformer AS t ON se.transformer_id = t.id
    JOIN transformer_type AS tt ON tt.id = t.type_id
    JOIN substation AS s ON se.substation_id = s.id
    WHERE se.is_transformer_low = 1
    AND (tt.id = 1 OR tt.id = 6 OR tt.id = 7 OR tt.id = 8)
    AND t.is_auxiliary = 0
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, s.zone
    ) AS T_tr
    LEFT JOIN 
    (
    -- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
    SELECT MW.date_time, s.zone, sum(MW.value) AS gen_MW
    -- f.is_generation, se.is_feeder
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN feeder AS f ON se.feeder_id = f.id
    JOIN substation AS s ON se.substation_id = s.id
    -- JOIN zone AS z ON z.id = s.zone
    -- JOIN gmd ON gmd.id = s.gmd
    WHERE f.is_generation = 1
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, s.zone
    ) AS T_gen
    ON T_tr.zone = T_gen.zone AND T_tr.date_time = T_gen.date_time
    ) AS T_zt
    GROUP BY T_zt.zone
    ) AS T_zt_max
    
    
    INNER JOIN 
    
    
    (
    SELECT T_tr.zone, T_tr.date_time, tr_MW+IFNULL(T_gen.gen_MW,0) AS zt
    FROM
    (
    SELECT s.zone, MW.date_time, sum(MW.value) as tr_MW
     -- tt.voltage_level, se.is_transformer_low
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN transformer AS t ON se.transformer_id = t.id
    JOIN transformer_type AS tt ON tt.id = t.type_id
    JOIN substation AS s ON se.substation_id = s.id
    WHERE se.is_transformer_low = 1
    AND (tt.id = 1 OR tt.id = 6 OR tt.id = 7 OR tt.id = 8)
    AND t.is_auxiliary = 0
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, s.zone
    ) AS T_tr
    LEFT JOIN 
    (
    -- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
    SELECT MW.date_time, s.zone, sum(MW.value) AS gen_MW
    -- f.is_generation, se.is_feeder
    FROM mega_watt AS MW
    JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
    JOIN feeder AS f ON se.feeder_id = f.id
    JOIN substation AS s ON se.substation_id = s.id
    -- JOIN zone AS z ON z.id = s.zone
    -- JOIN gmd ON gmd.id = s.gmd
    WHERE f.is_generation = 1
    AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
    GROUP BY MW.date_time, s.zone
    ) AS T_gen
    ON T_tr.zone = T_gen.zone AND T_tr.date_time = T_gen.date_time
    ) AS T_zt
    
    ON T_zt.zone = T_zt_max.zone AND T_zt.zt = T_zt_max.zt_max
    JOIN zone ON zone.id = T_zt.zone
    """

    max_zt_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    max_zt_df.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return max_zt_df


df = ss_max_min_voltage(from_datetime_str='2022-3-1 00:00', to_datetime_str='2022-7-30 23:00',
                        excel_path='max_zt_mw.xlsx')
