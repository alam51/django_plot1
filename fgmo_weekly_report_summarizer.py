import pandas as pd
import os
from dateutil.parser import parse

folder_path = r'C:\Users\user\Downloads\fwdfgmo'
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
            file_date = parse(file, dayfirst=True, yearfirst=False, fuzzy=True)
            file_date_str = str(file_date)[0:10]
            comment_df_new_series.columns = [file_date_str]
            for i in comment_df_new_series.index:
                comment_df.loc[i, file_date_str] = comment_df_new_series[i]
    # for name in dirs:
    #     print(os.path.join(root, name))

a = 1
comment_df.to_excel('FGMO_Report_comment.xlsx')


