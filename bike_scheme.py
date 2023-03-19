from ast import Not
import sheetioRio as rio
import pandas as pd
import numpy as np
import datetime

driver,sheeter = rio.apiconnect()
spreadsheet_id = "1XqOE-_3qDJQc6nCC6Nr1GKyg3HdKum2p4MZtt2F60v4"
sh_name = "Main"

df = rio.sheetsToDf(sheeter, spreadsheet_id, sh_name)
df = df[(df['erp cust code']!='') & (df['Month of enrollment']!='')]
req_cols = df[['Month of enrollment','erp cust code','Store Name','Unit id','unit name']]
# req_cols['Month of enrollment'] = pd.to_datetime(req_cols['Month of enrollment'])
req_cols = req_cols.drop_duplicates()


rows =[]
for i in req_cols.iterrows():
    c_code = i[1][1]
    store_name = i[1][2]
    unit_id = i[1][3]
    unit_name = i[1][4]
    if i[1][0] != None:
        start_date = datetime.datetime.strptime(i[1][0],'%m/%d/%Y').date()
        this_month = datetime.datetime.today().date().replace(day=1)
        start_month = start_date.month
        current_month = this_month.month
    
        for i in range(start_month,current_month+1):
            coming_months = start_date.replace(month=i)
            rows.append([coming_months,c_code,store_name,unit_id,unit_name])
    
ccode_mon = pd.DataFrame(rows,columns=['Pseudo Sale Date','c_code','cust_name','n_distributor_id','distributor_name'])
ccode_mon['zero_sale']=0


if not ccode_mon.empty:
    final_df2 = ccode_mon.astype(str)
    sheet_id = rio.dftoSheetsfast(driver,sheeter,final_df2,
                       sp_nam_id="1XqOE-_3qDJQc6nCC6Nr1GKyg3HdKum2p4MZtt2F60v4",
                       sh_name = "cust with enrollement date(Tableau)",resize_cols = False, resize_rows = False)
    print('sheet Updated')
else:
    print("empty sheet")




