
import tableauserverclient as TSC
from sqlalchemy import create_engine,text
import pandas as pd
import json
import re
import os
import time
import numpy as np



def tableauConnect():
    tableau_auth = TSC.TableauAuth('rio_pub', 'D73z2YVdwL6esykw6gKWHjsW')
    server = TSC.Server('https://tableau.ahwspl.net',use_server_version=True)
    return tableau_auth,server

def tableau2csv(req_view,filters = None):
    tableau_auth,server = tableauConnect()
    add_name = ''
    csv_req_option = TSC.CSVRequestOptions()
    if filters:
        for param,value in filters.items():
            csv_req_option.vf(param,value)
        add_name = '_'+ json.dumps(filters).strip('{}').replace('"','')
    with server.auth.sign_in(tableau_auth):    
        for v in TSC.Pager(server.views.get):
            if(v.name == req_view):
                req_view_id = v.id
                break
        views = filter(lambda x: x.id == req_view_id,
                               TSC.Pager(server.views.get))
        views = list(views)
        view = views.pop()
        print(view.id,view.name)
        server.views.populate_csv(view,csv_req_option)
        req_view = re.sub(r'\W+', '', req_view)
        with open('./{}{}.csv'.format(req_view,add_name), 'wb') as f:
            f.write(b''.join(view.csv))   
    server.auth.sign_out()
    return './{}{}.csv'.format(req_view,add_name)

def tableau2df(req_view,filters = None):
    csv_name = tableau2csv(req_view,filters)
    df = pd.read_csv(csv_name,index_col = None)
    os.remove(csv_name)
    return df


    
df  = tableau2df('LLP-Slab-Segments')


df = pd.read_csv('./SlabSegments.csv')    
df = df.reset_index()

print('DataFrame Created')
print('dataframe_size - ',df.shape)

df['Campaign Type'] = 'LivEasy SMSes Slab'
df['User Type'] = 1
df['Last Updated Time'] = time.time()
df_ordered = df[['rio_ret_id','ret_city','rio segment id','Segment','Campaign Type','User Type','Last Updated Time',
         'ret_common_name','Next Slab Rewards','Frontend LLP points',  'Points Req for Next Slab','Next Slab'
       ]]
meta_data = df_ordered[[ 'ret_common_name','Next Slab Rewards','Frontend LLP points',  'Points Req for Next Slab','Next Slab']]
meta_data.columns = ['RetailerName','NextSlabRewards','PtsAvailable','PtsReqNextSlab','NextBadge']

# meta_data['RetailerName'] = meta_data['RetailerName'].replace("'",'',regex=True)
# meta_data['RetailerName'] = meta_data['RetailerName'].replace(";",'',regex=True)
# meta_data['RetailerName'] = meta_data['RetailerName'].replace(":",'',regex=True)

meta_data['RetailerName'] = meta_data['RetailerName'].apply(lambda x: re.sub('[^a-zA-Z0-9 ]',"",x))


meta_data['PtsAvailable'] = meta_data['PtsAvailable'].astype(int)
meta_data['PtsReqNextSlab'] = meta_data['PtsReqNextSlab'].astype(int)
# meta_data['NextSlabRewards'] = meta_data['NextSlabRewards'].replace('/',',',regex=True)

jsondata = json.loads(meta_data.to_json(orient = 'records'))

print(jsondata)
df_ordered['metadata'] =jsondata
df_ordered = df_ordered.loc[df['rio segment id'].notnull()]
final_data = df_ordered.drop(columns=['ret_common_name','Next Slab Rewards','Frontend LLP points',  'Points Req for Next Slab','Next Slab'],axis=1)


final_data['metadata'] = final_data[['metadata']].applymap(str)
for i in range(len(final_data)):
    final_data['metadata'][i] = final_data['metadata'][i].replace("'",'"')
    final_data['metadata'][i] = final_data['metadata'][i].replace("\\n",'')

final_data = final_data.fillna("")

final_data_list=list(zip(*[final_data[col].values.tolist() for col in final_data.columns]))

engine = create_engine('postgresql://kartikraj_v_ro:xduLWBJDW4uGnFRS2GT6npDJjeHb@rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com:5439/skull')
con=engine.connect()
# con = db_connect('kartikraj_v_ro','xduLWBJDW4uGnFRS2GT6npDJjeHb','rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com','5439','skull')     

p_query = 'DELETE from rio_analytics.RIO_Campaign_Audience_DayWise where rio_segment_id in (272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313)'
con.execute(p_query)


insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list)[1:-1]+';'
con.execution_options(stream_results=False).execute(text(insert_query))


# index=[i for i in range(0,len(final_data_list),10000)]
# for i in range(len(index)):
#     if(index[i]!=index[-1]):
#         print(index[i],index[i+1])
#         insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list[index[i]:index[i+1]])[1:-1]+';'
#         con.execute(insert_query)
#     else:
#         print(index[i])
#         insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list[index[i]:])[1:-1]+';'
#         con.execute(insert_query)
 

""" JOINED but no Order"""
df  = tableau2df('Joined but not ordered')

print(df.head(5))
df=df[df['rio_ret_id']!='All']
df['RetailerName'] = df['ret_common_name']
df.drop('ret_common_name',axis=1,inplace=True)
df['ret_city']=''
df['rio_segment_id']=328
df['Segment'] = 'Joined but no order'
df['Campaign Type'] = 'LivEasy SMSes Slab'
df['user_type'] = 1
df['Last Updated Time'] = time.time()
df = df[['rio_ret_id','ret_city','rio_segment_id','Segment','Campaign Type','user_type','Last Updated Time','RetailerName']]
meta_data = df[['RetailerName']]
meta_data['RetailerName'] = meta_data['RetailerName'].apply(lambda x: re.sub('[^a-zA-Z0-9 ]',"",x))
jsondata = json.loads(meta_data.to_json(orient = 'records'))
df['metadata'] =jsondata
df['metadata'] = df[['metadata']].applymap(str)
df.drop('RetailerName',inplace=True,axis=1)
final_data = df.fillna("")
final_data['metadata'] = final_data['metadata'].replace("'",'"',regex=True)
# final_data.to_csv('joined_no_order.csv',index=False)
final_data_list=list(zip(*[final_data[col].values.tolist() for col in final_data.columns]))

engine = create_engine('postgresql://kartikraj_v_ro:xduLWBJDW4uGnFRS2GT6npDJjeHb@rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com:5439/skull')
con=engine.connect()
# con = db_connect('kartikraj_v_ro','xduLWBJDW4uGnFRS2GT6npDJjeHb','rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com','5439','skull')     

p_query = 'DELETE from rio_analytics.RIO_Campaign_Audience_DayWise where rio_segment_id in (328,329,330)'
con.execute(p_query)


insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list)[1:-1]+';'
con.execution_options(stream_results=False).execute(text(insert_query))

"""Ordered but not joined"""
df  = tableau2df('ordered but not joined')

print(df.head(5))
df=df[df['rio_ret_id']!='All']
df['RetailerName'] = df['ret_common_name']
df.drop('ret_common_name',axis=1,inplace=True)
df['ret_city']=''
df['rio_segment_id']=329
df['Segment'] = 'Ordered but not Joined'
df['Campaign Type'] = 'LivEasy SMSes Slab'
df['user_type'] = 1
df['Last Updated Time'] = time.time()
df = df[['rio_ret_id','ret_city','rio_segment_id','Segment','Campaign Type','user_type','Last Updated Time','RetailerName']]
meta_data = df[['RetailerName']]
meta_data['RetailerName'] = meta_data['RetailerName'].apply(lambda x: re.sub('[^a-zA-Z0-9 ]',"",x))
jsondata = json.loads(meta_data.to_json(orient = 'records'))
df['metadata'] =jsondata
df['metadata'] = df[['metadata']].applymap(str)
df.drop('RetailerName',inplace=True,axis=1)
final_data = df.fillna("")
final_data['metadata'] = final_data['metadata'].replace("'",'"',regex=True)
# final_data.to_csv('joined_no_order.csv',index=False)
final_data_list=list(zip(*[final_data[col].values.tolist() for col in final_data.columns]))

engine = create_engine('postgresql://kartikraj_v_ro:xduLWBJDW4uGnFRS2GT6npDJjeHb@rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com:5439/skull')
con=engine.connect()
# con = db_connect('kartikraj_v_ro','xduLWBJDW4uGnFRS2GT6npDJjeHb','rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com','5439','skull')     


insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list)[1:-1]+';'
con.execution_options(stream_results=False).execute(text(insert_query))

"""not ordered and not joined"""

df  = tableau2df('not ordered and not joined')

print(df.head(5))
df=df[df['rio_ret_id']!='All']
df['RetailerName'] = df['ret_common_name']
df.drop('ret_common_name',axis=1,inplace=True)
df['ret_city']=''
df['rio_segment_id']=330
df['Segment'] = 'not ordered and not joined'
df['Campaign Type'] = 'LivEasy SMSes Slab'
df['user_type'] = 1
df['Last Updated Time'] = time.time()
df = df[['rio_ret_id','ret_city','rio_segment_id','Segment','Campaign Type','user_type','Last Updated Time','RetailerName']]
meta_data = df[['RetailerName']]
meta_data['RetailerName'] = meta_data['RetailerName'].apply(lambda x: re.sub('[^a-zA-Z0-9 ]',"",x))
jsondata = json.loads(meta_data.to_json(orient = 'records'))
df['metadata'] =jsondata
df['metadata'] = df[['metadata']].applymap(str)
df.drop('RetailerName',inplace=True,axis=1)
final_data = df.fillna("")
final_data['metadata'] = final_data['metadata'].replace("'",'"',regex=True)
# final_data.to_csv('joined_no_order.csv',index=False)
final_data_list=list(zip(*[final_data[col].values.tolist() for col in final_data.columns]))

engine = create_engine('postgresql://kartikraj_v_ro:xduLWBJDW4uGnFRS2GT6npDJjeHb@rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com:5439/skull')
con=engine.connect()
# con = db_connect('kartikraj_v_ro','xduLWBJDW4uGnFRS2GT6npDJjeHb','rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com','5439','skull')     



insert_query = 'INSERT INTO rio_analytics.RIO_Campaign_Audience_DayWise (User_ID,User_City,RIO_Segment_ID,Segment_Name,Campaign_Type,User_Type,Last_Update_Time,metadata) VALUES '+str(final_data_list)[1:-1]+';'
con.execution_options(stream_results=False).execute(text(insert_query))





