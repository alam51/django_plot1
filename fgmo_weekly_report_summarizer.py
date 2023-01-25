import pandas as pd
import os
from dateutil.parser import parse

folder_path = r'I:\My Drive\IMD\Analysis\FGMO\Weekly Report Analysis\FGMO Weekly Reports\From CR'
comment_df = pd.DataFrame()
sr_df = pd.DataFrame()

for root, dirs, files in os.walk(folder_path, topdown=False):
    for file in files:
        absolute_file_path = os.path.join(root, file)
        print(absolute_file_path)
        a = 4
        if file.endswith(".xlsx") or file.endswith(".xls"):
            df1 = pd.read_excel(absolute_file_path, skiprows=4, nrows=43, usecols="B:J", index_col=0)
            df2 = df1.iloc[1:, :]
            comment_df_new_series = df2.loc[:, df2.columns[-1]]
            sr_df_new_series = df2.loc[:, 'SR at 50 Hz']
            file_date = parse(file, dayfirst=True, yearfirst=False, fuzzy=True)
            file_date_str = str(file_date)[0:10]
            comment_df_new_series.columns = [file_date_str]
            sr_df_new_series.columns = [file_date_str]
            for i in comment_df_new_series.index:
                comment_df.loc[i, file_date_str] = comment_df_new_series[i]
                sr_df.loc[i, file_date_str] = sr_df_new_series[i]

comment_df_t = comment_df.T
sr_df_t = sr_df.T
comment_sr_df_cols = pd.MultiIndex.from_product([sr_df_t.columns, ['comment', 'sr']], names=['date', 'cmnt_sr'])
# comment_sr_df_cols = list(zip(sr_df.columns.to_list(), ['comment', 'sr']))
comment_sr_df = pd.DataFrame(columns=comment_sr_df_cols)

for i in sr_df_t.index:
    for j in sr_df_t.columns:
        comment_sr_df.loc[i, (j, 'comment')] = comment_df_t.loc[i, j]
        comment_sr_df.loc[i, (j, 'sr')] = sr_df_t.loc[i, j]

    # for name in dirs:
    #     print(os.path.join(root, name))

a = 1
comment_df.to_excel('FGMO_Report_comment.xlsx')
sr_df.to_excel('FGMO_Report_SR.xlsx')
comment_sr_df.to_excel('FGMO_Report_Comment_SR.xlsx')
