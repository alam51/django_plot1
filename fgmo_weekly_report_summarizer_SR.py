import pandas as pd
import os
from dateutil.parser import parse

folder_path = r'I:\My Drive\IMD\Analysis\FGMO\Weekly Report Analysis\FGMO Weekly Reports\From CR'
# folder_path = r'I:\My Drive\IMD\Analysis\FGMO\Weekly Report Analysis\FGMO Weekly Reports\From CR\FGMO 2021\January\test'
op_df = pd.DataFrame()
# sr_df = pd.DataFrame()

for root, dirs, files in os.walk(folder_path, topdown=False):
    for file in files:
        absolute_file_path = os.path.join(root, file)
        print(absolute_file_path)
        a = 4
        if (file.endswith(".xlsx") or file.endswith(".xls")) and '$' not in file:
            df = pd.read_excel(absolute_file_path, index_col=None, header=None)
            file_date = parse(file, dayfirst=True, yearfirst=False, fuzzy=True)
            file_date_str = str(file_date)[0:10]

            for i, row in enumerate(df.index):
                for j, col in enumerate(df.columns):
                    current_cell = df.loc[row, col]
                    if type(current_cell) == str:
                        if current_cell.lower().endswith('plants running on fgmo') or \
                                current_cell.lower().endswith('total number of plants on fgmo'):
                            op_df.loc[file_date_str, 'plant_count'] = df.loc[df.index[i+1], df.columns[j]]

                        elif current_cell.lower().endswith('total spinning reserve') or \
                                current_cell.lower().endswith('total spinning reserve'):
                            op_df.loc[file_date_str, 'sr'] = df.loc[df.index[i+1], df.columns[j]]


op_df.to_excel('FGMO_Report_SR_count.xlsx')
# sr_df.to_excel('FGMO_Report_SR.xlsx')
# comment_sr_df.to_excel('FGMO_Report_Comment_SR.xlsx')
