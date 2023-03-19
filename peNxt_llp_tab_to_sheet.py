from pyparsing import Regex
import sheetioRio as rio
import pandas as pd
import datetime as dt


driver, sheeter = rio.apiconnect()

# df_from_sheet = rio.sheetsToDf(sheeter,spreadsheet_id='1lzVgM7gyxKNnwesDkOUvfda9Up0XZZNKshzYhelpolg',sh_name = 'Master')
# df_from_sheet = df_from_sheet[['RioRetailerID']].drop_duplicates()
# df_from_sheet.dropna(inplace=True)
# df_from_sheet['RioRetailerID'] = df_from_sheet['RioRetailerID'].astype(int)

# # list_ = df_from_sheet['RioRetailerID'].to_list()
# # print(list_)

# df_tableau = rio.tableau2df('For Points Upload')
# monthwise_tableau = rio.tableau2df('Points month wise')

# print('dataframe created from Tableau')

# monthwise_tableau = monthwise_tableau[monthwise_tableau['Measure Names']=='Net Sales']
# monthwise_tableau.drop(['Active','Measure Names'],1,inplace=True)
# monthwise_tableau.rename(columns={'Measure Values':'Net Sales','Month of order_date':'Month'},inplace=True)

# monthwise_tableau = monthwise_tableau[['rio_ret_id','Month','Net Sales']]
# monthwise_tableau['Mrp Sales']=''

# print('df prepared for sheet1')


# llp_points = df_tableau[df_tableau['Measure Names']=='Calculated LLP points']
# llp_points1 = llp_points.drop(['Measure Names','Active'],axis=1)
# llp_points1.rename(columns = {'Measure Values':'LLP Earned Points Total'},inplace=True)
# # print(llp_points1.head())

# redeem_df = df_tableau[df_tableau['Measure Names']=='Total Redeemed Points ']
# redeem_df1 = redeem_df.drop(['Measure Names','Active'],axis=1)
# redeem_df1.rename(columns = {'Measure Values':'Redeemed Points'},inplace=True)
# # print(redeem_df1.head())

# final_df = pd.merge(llp_points1,redeem_df1,on='rio_ret_id',how='left')
# print('df prepared for sheet2')

# df_from_sheet2 = df_from_sheet.copy()
# df_from_sheet2['RioRetailerID'] = df_from_sheet2['RioRetailerID'].astype(str)


# join_wd_sheet2 = pd.merge(df_from_sheet2,final_df,left_on='RioRetailerID', right_on='rio_ret_id',how='left')
# join_wd_sheet2.drop('rio_ret_id',axis=1,inplace=True)
# join_wd_sheet2.fillna('',inplace=True)

# print('joined with sheet2')


# join_wd_sheet1 = pd.merge(df_from_sheet,monthwise_tableau,left_on='RioRetailerID',right_on='rio_ret_id',how='left')
# join_wd_sheet1.drop('rio_ret_id',axis=1,inplace=True)
# join_wd_sheet1.fillna('',inplace=True)

# print('joined with sheet1')


# if not join_wd_sheet2.empty:
#     dfel = join_wd_sheet2.astype(str)
#     sheet_id = rio.dftoSheetsfast(driver,sheeter,dfel,
#                        sp_nam_id='1elQ8XcJTi5mhHfH1LrEIiK-FvYhMz1rS4nqILOk_Lfs',
#                        sh_name = 'Sheet2',resize_cols = False, resize_rows = False)
#     print('peNxt sheet2 updated')
#     print(sheet_id)
# else:
#     print("sheet2 empty dataframe")

# if not join_wd_sheet1.empty:
#     dfel = join_wd_sheet1.astype(str)
#     sheet_id = rio.dftoSheetsfast(driver,sheeter,dfel,
#                        sp_nam_id='1elQ8XcJTi5mhHfH1LrEIiK-FvYhMz1rS4nqILOk_Lfs',
#                        sh_name = 'Sheet1',resize_cols = False, resize_rows = False)
#     print('peNxt sheet1 updated')
#     print(sheet_id)
# else:
#     print("sheet1 empty dataframe")

# print('done')

########################### New #################3

df_tableau = rio.tableau2df('Liveasy points info')

print(df_tableau.head(5))


net_sale_df = df_tableau[df_tableau['Measure Names']=='Net Sales']
net_sale_df.rename(columns = {'Measure Values':'net_sales'},inplace=True)
net_sale_df.drop('Measure Names',axis=1,inplace=True)
net_sale_df.reset_index()
print(net_sale_df.head(5))



redeemed_df = df_tableau[df_tableau['Measure Names']=='Total Redeemed Points ']
redeemed_df.rename(columns = {'Measure Values':'Redeemed Points'},inplace=True)
redeemed_df.drop('Measure Names',axis=1,inplace=True)
redeemed_df = redeemed_df[['rio_ret_id','Redeemed Points']]
redeemed_df.reset_index()
print(redeemed_df.head(5))

final = pd.merge(net_sale_df,redeemed_df,how='inner',on='rio_ret_id')
print(final.head(5))

final = final[['rio_ret_id','Enrollment Identifier','is_rio_registrered','net_sales','Redeemed Points']]

if not final.empty:
    dfel = final.astype(str)
    sheet_id = rio.dftoSheetsfast(driver,sheeter,dfel,
                       sp_nam_id='1Nj8cM5zUbBe2xKCanT5ObEjVDZfLnxy7ge8llnMMTek',
                       sh_name = 'llp',resize_cols = False, resize_rows = False)
    print('llp points retailer level sheet updated')
    print(sheet_id)
else:
    print("sheet2 empty dataframe")