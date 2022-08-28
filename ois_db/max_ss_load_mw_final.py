import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def max_ss_load_mw(from_datetime_str, to_datetime_str, excel_path=r'max_ss_load_mw.xlsx', mw_thresh = 350,
                   from_hour1=None,
                   to_hour1=None, from_hour2=None, to_hour2=None):
    max_zt_query_str = f"""
SELECT s.name, T_ss_MW1.* FROM 
(
SELECT T_ss_MW.id, MAX(T_ss_MW.ss_MW) AS ss_MW FROM 
(
SELECT T_tr.id, T_tr.date_time, tr_MW+IFNULL(T_gen.gen_MW,0) AS ss_MW
FROM
(
SELECT s.id, MW.date_time, sum(MW.value) as tr_MW
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
-- AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) OR
-- (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))
GROUP BY MW.date_time, s.id
) AS T_tr
LEFT JOIN 
(
-- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
SELECT MW.date_time, s.id, sum(MW.value) AS gen_MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id

WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}' 
-- AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) OR
-- (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))
GROUP BY MW.date_time, s.id
) AS T_gen
ON T_tr.id = T_gen.id AND T_tr.date_time = T_gen.date_time
) AS T_ss_MW

WHERE ss_MW < {mw_thresh}
GROUP BY T_ss_MW.id
) AS T_ss_MW_max


INNER JOIN 


(
SELECT T_tr.id, T_tr.date_time, tr_MW+IFNULL(T_gen.gen_MW,0) AS ss_MW
FROM
(
SELECT s.id, MW.date_time, sum(MW.value) as tr_MW
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
-- AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) OR
-- (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))
GROUP BY MW.date_time, s.id
) AS T_tr
LEFT JOIN 
(
-- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
SELECT MW.date_time, s.id, sum(MW.value) AS gen_MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id
-- JOIN zone AS z ON z.id = s.zone
-- JOIN gmd ON gmd.id = s.gmd
WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}' 
-- AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) OR
-- (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))
GROUP BY MW.date_time, s.id
) AS T_gen
ON T_tr.id = T_gen.id AND T_tr.date_time = T_gen.date_time
) AS T_ss_MW1

ON T_ss_MW1.id = T_ss_MW_max.id AND T_ss_MW1.ss_MW = T_ss_MW_max.ss_MW

JOIN substation AS s ON s.id = T_ss_MW1.id
GROUP BY s.id
ORDER BY 1 
"""
    max_min_kv_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    max_min_kv_df.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return max_min_kv_df


df = max_ss_load_mw(from_datetime_str='2022-7-01 00:00', to_datetime_str='2022-7-31 23:00',
                    excel_path='max_ss_load_mw.xlsx')
