a = 4
import pandas as pd
from max_ss_load_mw_final import max_ss_load_mw
# date_range.dr

previous_df = pd.DataFrame()
for month in range(2, 3):
    date_range = pd.date_range(start=f'2022-{month}-1', periods=5, freq='7D').tolist()
    date_range[4] = pd.to_datetime(f'2022-{month + 1}-1') - pd.to_timedelta('1min')
    for i, date in enumerate(date_range[:len(date_range)-1]):
        df = max_ss_load_mw(from_datetime_str=date_range[i],
                            to_datetime_str=date_range[i+1],
                            from_hour1=8,
                            to_hour1=12)
        # cols = df.columns
        # new_cols = cols[: len(cols-1)] + f''
        # df.columns = cols[: len(cols)]
        if previous_df.empty:
            previous_df = df
        else:
            previous_df.loc[:, f'month{month}_week{i}_max'] = df.loc[:, 'ss_MW']
            # previous_df.loc[:, f'month{month}_week{i}_max_time'] = df.loc[:, 'date_time']
previous_df.to_excel('day_peak.xlsx')

for month in range(1, 5):
    date_range = pd.date_range(start=f'2022-{month}-1', periods=5, freq='7D').tolist()
    date_range[4] = pd.to_datetime(f'2022-{month + 1}-1') - pd.to_timedelta('1min')
    for i, date in enumerate(date_range[:len(date_range)-1]):
        df = max_ss_load_mw(from_datetime_str=date_range[i],
                            to_datetime_str=date_range[i+1],
                            from_hour1=18,
                            to_hour1=22)
        # cols = df.columns
        # new_cols = cols[: len(cols-1)] + f''
        # df.columns = cols[: len(cols)]
        if previous_df.empty:
            previous_df = df
        else:
            previous_df.loc[:, f'month{month}_week{i}_max'] = df.loc[:, 'ss_MW']
            # previous_df.loc[:, f'month{month}_week{i}_max_time'] = df.loc[:, 'date_time']
previous_df.to_excel('evening_peak.xlsx')

for month in range(1, 5):
    date_range = pd.date_range(start=f'2022-{month}-1', periods=5, freq='7D').tolist()
    date_range[4] = pd.to_datetime(f'2022-{month + 1}-1') - pd.to_timedelta('1min')
    for i, date in enumerate(date_range[:len(date_range)-1]):
        df = max_ss_load_mw(from_datetime_str=date_range[i],
                            to_datetime_str=date_range[i+1],
                            from_hour1=23,
                            to_hour1=23,
                            from_hour2=0,
                            to_hour2=7)
        # cols = df.columns
        # new_cols = cols[: len(cols-1)] + f''
        # df.columns = cols[: len(cols)]
        if previous_df.empty:
            previous_df = df
        else:
            previous_df.loc[:, f'month{month}_week{i+1}_max'] = df.loc[:, 'ss_MW']
            # previous_df.loc[:, f'month{month}_week{i}_max_time'] = df.loc[:, 'date_time']
previous_df.to_excel('irrigation_peak.xlsx')

a = 4
# print(date_range)
# print(date_range[0])
