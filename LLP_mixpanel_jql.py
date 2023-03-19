#!/usr/bin/env python
# coding: utf-8

# In[253]:


from datetime import datetime, timedelta
from datetime import date
payload1 = "script=function%20main%28%29%20%7B%20%20%20return%20Events%28%7B%20%20%20%20%20from_date%3A%20%27"
payload2="%27%2C%20%20%20%20%20to_date%3A%20%27"
payload3="%27%2C%20%20%20%20%20event_selectors%3A%20%5B%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Click%20Widget-Card%27%2C%20selector%3A%20%27properties%5B%22Widget%20ID%22%5D%3D%3D197%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Click%27%2C%20selector%3A%27properties%5B%22Icon%22%5D%3D%3D%22Liveasy%20Repeat%20User%20Banner%22%27%20%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Click%27%2C%20selector%3A%27properties%5B%22Icon%22%5D%3D%3D%22Liveasy%20New%20User%20Banner%22%27%20%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Visited%27%2C%20selector%3A%27properties%5B%22Category%22%5D%3D%3D%22Liveasy%20enrolled%20user%20details%20page%22%27%20%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Visited%27%2C%20selector%3A%27properties%5B%22Category%22%5D%3D%3D%22Liveasy%20new%20user%20details%20page%22%27%20%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Visited%27%2C%20selector%3A%27properties%5B%22Category%22%5D%3D%3D%22RPL-LLP%22%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Visited%27%2C%20selector%3A%27properties%5B%22Category%22%5D%3D%3D%22RPL-Whatsapp%22%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20explore%20liveasy%20products%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20Liveasy%20FAQ%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20Liveasy%20popular%20product%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20Liveasy%20TandC%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20Liveasy%20view%20all%20rewards%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Tapped%20explore%20liveasy%20products%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Liveasy%20reward%20cards%20swiped%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Click%20Banner%27%2Cselector%3A%27properties%5B%22Campaign%20Id%22%5D%3D%3D16834%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Inapp%20Clicked%27%2C%20selector%3A%27properties%5B%22Campaign%20Name%22%5D%3D%3D%22LivEasy%20Loyalty%20Program_2%22%27%20%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Click%20Push%20Notification%27%2C%20selector%3A%27properties%5B%22Notification%20Title%22%5D%3D%3D%22Join%20the%20LIVEASY%20Loyalty%20Program%20now%21%20%3Aheart_eyes%3A%22%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%27Click%20Widget-Card%27%2Cselector%3A%27properties%5B%22Widget%20ID%22%5D%3D%3D233%27%7D%2C%20%20%20%20%20%20%20%20%20%7Bevent%3A%20%27Add%20to%20cart%27%2C%20selector%3A%27properties%5B%22Sub%20Origin%22%5D%3D%3D%22Liveasy%20Offer%20Details%20Page%22%27%20%7D%2C%20%20%20%20%20%5D%20%20%20%7D%29%20%20%20.map%28function%28event%29%7B%20%20%20%20%20%20return%20%7B%20%20%20%20%20%20%20%20%22Date1%22%3A%20event.time%2C%20%20%20%20%20%20%20%20%22Date%22%20%3A%20new%20Date%28event.time%29.toISOString%28%29.substr%280%2C19%29%2C%20%20%20%20%20%20%20%20%22event%22%20%3A%20event.name%2C%20%20%20%20%20%20%20%20%20%27Icon%27%3Aevent.properties.Icon%2C%20%20%20%20%20%20%20%20%20%27Category%27%3Aevent.properties.Category%2C%20%20%20%20%20%20%20%20%20%22Campaign_Id%22%3Aevent.properties%5B%22Campaign%20Id%22%5D%2C%20%20%20%20%20%20%20%20%27Name%27%3A%20event.properties.Name%2C%20%20%20%20%20%20%20%20%27mobile_brand%27%3Aevent.properties.%24brand%2C%20%20%20%20%20%20%20%20%27insert_Id%27%3Aevent.properties%5B%22%24insert_id%22%5D%2C%20%20%20%20%20%20%20%20%22city%22%20%3A%20event.properties.%24city%2C%20%20%20%20%20%20%20%20%22Oms_ID%22%3A%20event.properties%5B%22Oms%20Id%22%5D%2C%20%20%20%20%20%20%20%20%27Region%27%3A%20event.properties.%24region%2C%20%20%20%20%20%20%20%20%27Mobile_Number%27%20%3A%20event.properties%5B%22Mobile%20Number%22%5D%2C%20%20%20%20%20%20%20%20%27Sub_origin%27%3Aevent.properties%5B%22Sub%20Origin%22%5D%20%20%20%20%20%20%7D%3B%20%20%20%20%7D%29%3B%20%7D"
today = date.today()
d1=date.today()- timedelta(days = 5)
d1=d1.strftime("%Y-%m-%d")
today=today.strftime("%Y-%m-%d")
payload=payload1+d1+payload2+today+payload3
print(d1)
print(today)
print(payload)


# In[254]:


import requests

url = "https://mixpanel.com/api/2.0/jql?project_id=2039104"

#payload = "script=function%20main%28%29%20%7B%20%20%20%20return%20Events%28%7B%20%20%20%20%20%20from_date%3A%20params.start_date%2C%20%20%20%20%20%20to_date%3A%20%20%20params.end_date%20%20%20%20%7D%29.filter%28function%28event%29%20%7B%20return%20%28event.name%20%3D%3D%20params.event%5B0%5D%20%26%20%28event.properties.Icon%3D%3Dparams.icon%5B0%5D%20%7C%20event.properties.Icon%3D%3Dparams.icon%5B1%5D%29%29%20%7C%20%20%20%20%28event.name%20%3D%3D%20params.event%5B1%5D%20%26%20%28event.properties.Category%3D%3Dparams.icon%5B2%5D%20%7C%20event.properties.Category%3D%3Dparams.icon%5B3%5D%29%29%7C%20%20%20%20%28event.name%20%3D%3D%20params.event%5B2%5D%20%26%20event.properties%5B%22Campaign%20Id%22%5D%3D%3D16834%29%7C%20%20%20%20%28event.name%3D%3Dparams.event%5B3%5D%29%7C%20%20%20%20%28event.name%3D%3Dparams.event%5B4%5D%29%7C%20%20%20%20%28event.name%3D%3Dparams.event%5B5%5D%29%7C%20%20%20%20%28event.name%3D%3Dparams.event%5B6%5D%29%7C%20%20%20%20%28event.name%3D%3Dparams.event%5B7%5D%29%7C%28event.name%3D%3Dparams.event%5B8%5D%29%20%20%20%20%20%20%20%7D%29%20.map%28function%28event%29%7B%20%20%20%20%20%20%20return%20%7B%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%22Date1%22%3A%20event.time%2C%20%20%20%20%20%20%20%20%22Date%22%20%3A%20new%20Date%28event.time%29.toISOString%28%29.substr%280%2C19%29%2C%20%20%20%20%20%20%20%20%22event%22%20%3A%20event.name%2C%20%20%20%20%20%20%20%20%20%27Icon%27%3Aevent.properties.Icon%2C%20%20%20%20%20%20%20%20%20%27Category%27%3Aevent.properties.Category%2C%20%20%20%20%20%20%20%20%20%22Campaign_Id%22%3Aevent.properties%5B%22Campaign%20Id%22%5D%2C%20%20%20%20%20%20%20%20%27Name%27%3A%20event.properties.Name%2C%20%20%20%20%20%20%20%20%27mobile_brand%27%3Aevent.properties.%24brand%2C%20%20%20%20%20%20%20%20%27insert_Id%27%3Aevent.properties%5B%22%24insert_id%22%5D%2C%20%20%20%20%20%20%20%20%22city%22%20%3A%20event.properties.%24city%2C%20%20%20%20%20%20%20%20%22Oms_ID%22%3A%20event.properties%5B%22Oms%20Id%22%5D%2C%20%20%20%20%20%20%20%20%27Region%27%3A%20event.properties.%24region%2C%20%20%20%20%20%20%20%20%27Mobile_Number%27%20%3A%20event.properties%5B%22Mobile%20Number%22%5D%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%7D%3B%20%20%20%20%20%7D%29%3B%20%20%20%20%20%20%7D&params=%7B%0A%20%20%22start_date%22%3A%20%222021-12-20%22%2C%0A%20%20%22end_date%22%3A%20%222022-01-05%22%2C%0A%20%20%22event%22%3A%20%5B%22Click%22%2C%22Visited%22%2C%22Click%20Banner%22%2C%22Tapped%20Explore%20Liveasy%20Products%22%2C%22Tapped%20Liveasy%20FAQ%22%2C%22Tapped%20Liveasy%20Popular%20Product%22%2C%22Tapped%20Liveasy%20TandC%22%2C%22Tapped%20Liveasy%20view%20all%20rewards%22%2C%22Visit-Liveasy%20Enrolled%20User%20Details%20Page%20%22%2C%22Visit-Liveasy%20New%20User%20Details%20Page%22%5D%2C%0A%20%20%22icon%22%3A%5B%22Liveasy%20Repeat%20User%20Banner%22%2C%22Liveasy%20New%20User%20Banner%22%2C%22Liveasy%20enrolled%20user%20details%20page%22%2C%22Liveasy%20new%20user%20details%20page%22%5D%0A%7D"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "Basic c2F1cmFiaGpxbC5kMDAzZDUubXAtc2VydmljZS1hY2NvdW50OmhKUFVTYlJwNkpjcFByWWdwN3VXREpUY2szOTBGNTQ0"
}

response = requests.request("POST", url, data=payload, headers=headers)

#print(response.text)


# In[255]:


import pandas as pd
import math
import psycopg2


# In[256]:


df = pd.read_json(response.text,dtype={'Date1':'int64','Date':'str','event':'str','Icon':'str','Name':'str','mobile_brand':'str','insert_id':'str','city':'str','Oms_ID':'int','Region':'str','Mobile_Number':'str','Campaign_Id':'int','Category':'str','Sub_origin':'str'})


# In[257]:


print(min(df['Date1']))
delete_query="delete from rio_analytics.MixpanelJQL  where Date1>="+str(min(df['Date1']))+";"
delete_query
connection = psycopg2.connect(
host= "rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com",
port=5439,
dbname="skull",
user= "analytics_write_only",
password="!x46HiEp7*ZZY7ti")
cur = connection.cursor()
cur.execute(delete_query)
connection.commit()
#psycopg2.extras.execute_values(cur, new_query,list(map(lambda el:[el], max_timestamp)))
cur.close()
connection.close()


# In[258]:

if(('Campaign_Id') in(df.columns)):
    df['Campaign_Id']=df['Campaign_Id'].fillna(0)
    df['Campaign_Id']=df['Campaign_Id'].astype(int)
else:
    df['Campaign_Id']=0


# In[259]:


df['Category']=df['Category'].fillna(0)
df['Category'].replace('nan', '', inplace=True)


# In[260]:


##df


# In[261]:


df=df.dropna(subset=['Mobile_Number', 'Oms_ID'])
l=len(df.index)
#l


# In[262]:


divisor=10000
div=math.ceil(l/divisor)


# In[21]:


df.columns
data1=df


# In[263]:


list(df.columns)
def listToString(s): 
    
    # initialize an empty string
    str1 = "" 
    
    # traverse in the string  
    for ele in s: 
        str1 += ele  + " ,"
    
    # return string  
    return str1 
my_str=listToString(df.columns)
my_str = my_str[:-1]
my_str


# In[264]:


data1['Name']=data1['Name'].str.replace(r"[\']",r" ")


# In[265]:


#for i in (range(div)):
for i in range(div):
    data2=data1.iloc[i*divisor:(divisor*(i+1)),]
    print(i*divisor)
    print((divisor*(i+1))-1)
    data_to_db=list(zip(*[data2[col].values.tolist() for col in data2.columns]))
    thea_i_query = """INSERT INTO rio_analytics.MixpanelJQL 
    ("""+my_str+""") VALUES """+str(data_to_db)[1:-1]+';'
    connection = psycopg2.connect(
    host= "rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com",
    port=5439,
    dbname="skull",
    user= "analytics_write_only",
    password="!x46HiEp7*ZZY7ti")
    cur = connection.cursor()
    cur.execute(thea_i_query)
    connection.commit()
    #psycopg2.extras.execute_values(cur, new_query,list(map(lambda el:[el], max_timestamp)))
    cur.close()
    connection.close()


# In[ ]:


# create table rio_analytics.MixpanelJQL
# (
# Date1 bigint, 
# Date timestamp,
# event nvarchar(150),
# Icon nvarchar(250),
# Name nvarchar(500),
# mobile_brand nvarchar(250),
# insert_Id nvarchar(250),
# city nvarchar(250), 
# Oms_ID bigint, 
# Region nvarchar(250), 
# Mobile_Number bigint, 
# Campaign_Id bigint, 
# Category nvarchar(250)
# );
# grant all on rio_analytics.MixpanelJQL to public;
#alter table rio_analytics.MixpanelJQL 
#add Sub_origin nvarchar(300);

# In[ ]:




