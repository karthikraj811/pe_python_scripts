# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 13:31:22 2019

@author: Jaideep
"""
from __future__ import division
from datetime import datetime,timedelta 
import requests
import json
try:
    from urllib import quote  # Python 2.X
except ImportError:
    from urllib.parse import quote 
import psycopg2.extras
import sqlalchemy as db
import tableauserverclient as TSC
import pandas as pd
import re
import os
import math
import psycopg2
import pandas as pd
from time import sleep
# from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread
import boto3
import io
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from httplib2 import Http
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
import time

email = 'action_alerts@retailio.in'   # Google sheet Read and Write will be done by this email id's token
pwd = 'Welcome@2021'  

start = time.time()

def runtime():
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Script Run Time - {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))

    
def makePGConn(dbHost, dbPort, dbUser, dbPwd, dbName):
    return psycopg2.connect("host={} port={} dbname={} user={} password={}"
                            .format(dbHost, dbPort, dbName, dbUser, dbPwd))


def getSkull():
    return makePGConn('rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com', 5439, 'analytics_write_only', '!x46HiEp7*ZZY7ti', 'skull')

def getSkull_rio_read():
    return makePGConn('rio-redshift-prod.cnybds0fmh4l.ap-south-1.redshift.amazonaws.com', 5439, 'rio_read_only', 'Uqn$E&&z3XpeD9WJ', 'skull')

def getEngine(dbHost, dbPort, dbUser, dbPwd, dbName):
    print("Mysql connection established")
    return db.create_engine('mysql://{}:{}@{}:{}/{}'.format(dbUser,dbPwd,dbHost,dbPort,dbName))

def getOMS():
    return getEngine('omstransactionalprod-read.cw8vifcngblm.ap-south-1.rds.amazonaws.com', 3306, 'root', 'ocON2usIP9ir', 'omstransactional').connect()

def getOffer():
    return getEngine('offer-engine-prod-replica.cw8vifcngblm.ap-south-1.rds.amazonaws.com', 3306, 'support_read_only', 'Wfn2n9gYjb', '').connect()

def getOffertemp():
    return getEngine('offer-engine-prod-replica-new.cw8vifcngblm.ap-south-1.rds.amazonaws.com', 3306, 'support_read_only', 'Wfn2n9gYjb', '').connect()

def getInvoiceDB():
    return getEngine('invoice-service-read-replica.cw8vifcngblm.ap-south-1.rds.amazonaws.com', 3306, 'invoice_qa_read', 'r3ta1lwh1zz123', 'payment_service').connect()

def getCopsDB():
    return getEngine('cops-prod-read-replica.cw8vifcngblm.ap-south-1.rds.amazonaws.com', 3306, 'analytics_read', 'AbzVta8y8mqUANQM8p64UNWr5j6hmr2F', 'cops').connect()

def getPrismDB():
    #return getEngine('prism.cijfstakmq3t.ap-south-1.rds.amazonaws.com', 5432, 'atpiutlo', 'PhruapufpafJicCoispIryol', 'prism').connect()
    #psycopg2.pool.SimpleConnectionPool(1, 60,"host='prism.cijfstakmq3t.ap-south-1.rds.amazonaws.com' port=5432 dbname='prism' user='atpiutlo' password='PhruapufpafJicCoispIryol'")
    psycopg2.connect(host='prism.cijfstakmq3t.ap-south-1.rds.amazonaws.com',port='5432',dbname='prism',user='atpiutlo',password='PhruapufpafJicCoispIryol')
    
def tableauConnect():
    tableau_auth = TSC.TableauAuth('Pushmetrics', 'tilj5QuoddavDiGleuccemAd')
    server = TSC.Server('https://tableau.ahwspl.net',use_server_version=True)
    return tableau_auth,server

#Filter to be applied like:
# filter = {'filter_name_1':'filter_value_1',
#          'filter_name_2':'filter_value_2'}
# Example : 
#     f4 = {'Distributor Type':'Ascent',
#           'Thea Filter':'Non Thea'}


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

def tableau2gs(req_view,filters = None,sp_nam_id=None, sp_nam = None, sh_name='Sheet1', driver = None,sheeter = None,  resize_cols = True, resize_rows = True, append = False, start_row = 1, start_col = 1, folder_id = None, create= False):
    result = tableau2df(req_view,filters)
    if driver == None or driver == None:
        driver,sheeter = apiconnect()
    sheet_id = dftoSheetsfast(driver, sheeter, result, sp_nam_id=sp_nam_id, sp_nam = sp_nam, sh_name=sh_name,  resize_cols = resize_cols, resize_rows = resize_rows, append = append, start_row = start_row, start_col = start_col, folder_id = folder_id, create= create)    
    print('Output length: {}, Output Written in GS: {} '.format(len(result),sheet_id))
    return sheet_id


def tableau2pdf(req_view,filters = None):
    tableau_auth,server = tableauConnect()
    add_name = ''
    pdf_req_option = TSC.PDFRequestOptions()
    if filters:
        for param,value in filters.items():
            pdf_req_option.vf(param,value)
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
        server.views.populate_pdf(view,pdf_req_option)
        req_view = re.sub(r'\W+', '', req_view)
        with open('./{}{}.pdf'.format(req_view,add_name), 'wb') as f:
            f.write(view.pdf)   
    server.auth.sign_out()
    return './{}{}.pdf'.format(req_view,add_name)


def apiconnect():
    SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    store = file.Storage('token_rio_new.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('client_id_rio_new.json', SCOPES)
        creds = tools.run_flow(flow, store)
    driver = build('drive', 'v3', http=creds.authorize(Http()))
    sheeter = build('sheets', 'v4', http=creds.authorize(Http()))
    return driver, sheeter


def num_to_col(num):
    letters = ''
    x = None
    while num:
        mod = (num-1)%26
        letters +=chr(mod+65)
        num = (num-1)//26
        x = ''.join(reversed(letters))
    return x

def dftoSheetsfast(driver, sheeter, dframe, sp_nam_id=None, sp_nam = None, sh_name='Sheet1',  resize_cols = True, resize_rows = True, append = False, start_row = 1, start_col = 1, folder_id = None, create= False):
    # driver,sheeter = apiconnect()
    dframe.fillna('', inplace=True)
    g = dframe.columns.tolist()
    for i in g:
        if isinstance(dframe[i][0], dt.datetime) or dframe[i].dtype == 'datetime64[ns]':
            dframe[i] = dframe[i].apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
        elif isinstance(dframe[i][0], dt.date):
            dframe[i] = dframe[i].apply(lambda x: dt.date.strftime(x, '%Y-%m-%d'))
    keys = dframe.keys()
    x = dframe.values.tolist()
    y = x
    x.insert(0,keys.tolist())
    row, col = dframe.shape
    c = num_to_col(col+start_col-1)
    s_c = num_to_col(start_col)
    r = str(start_row+row)
    sheet_id = sp_nam_id

    body = {
        'values': x
    }

    # media = MediaFileUpload('15_53_Product Variant Rule.csv',
    #                         mimetype='text/csv',
    #                         resumable=True)
    if create:
        file_metadata = {
            'name': sp_nam,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }
        sheet = driver.files().create(body=file_metadata, fields='id').execute()
        sheet_id = sheet.get('id')
        addd = {
            "requests":[
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 0,
                            "title": sh_name
                        },
                        "fields": "title"
                    }
                }
            ],
        }
        sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=addd).execute()
        dated = sheeter.spreadsheets().values().update(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    elif append:
        body = {
            'values': y
        }
        dated = sheeter.spreadsheets().values().append(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    else:
        dated = sheeter.spreadsheets().values().update(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    sh = sheeter.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_dict = {}
    for i in range(len(sh['sheets'])):
        sheet_dict[sh['sheets'][i]['properties']['title']] = sh['sheets'][i]['properties']
    sh_name_id =sheet_dict[sh_name]['sheetId']
    if resize_cols:
        try:
            req = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sh_name_id,
                                "dimension": "COLUMNS",
                                "startIndex": col

                            }
                        }
                    }
                ],}
            sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=req).execute()
        except:
            pass
    if resize_rows:
        try:
            req = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sh_name_id,
                                "dimension": "ROWS",
                                "startIndex": row+1

                            }
                        }
                    }
            ],}
            sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=req).execute()
        except:
            pass

    return sheet_id


def sheetsToDf(sheeter, spreadsheet_id, sh_name):
    output = sheeter.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=sh_name).execute()
    result = pd.DataFrame(output['values'])
    col = (result.iloc[0])
    results = result.drop(0)
    results.columns = col
    # results = results.replace(None, '')
    # print(results.head())
    return results

def send_sms(df,campaign):
    if not df.empty:
        dfin = pd.DataFrame()
        df['mobile'] = df['mobile'].astype(int)
        length = len(df)
        print(length)
        for a,b in zip(df['mobile'],df['message']):
            js = quote(b)
            endp = "http://enterprise.smsgupshup.com/GatewayAPI/rest?msg="+js+"&v=1.1&userid=&password=&send_to=91+"+str(a)+"&msg_type=Unicode_Text&method=sendMessage&format=JSON"
            resp = requests.post(url = endp)
            jo = json.loads(resp.text)["response"]
            print(jo)
            d = pd.DataFrame(jo, index=[0])
            dfin = dfin.append(d,ignore_index= True)
           
        dfin['mobile'] = dfin['phone'].str[2:].astype(int)
        dfin = dfin.merge(df,on=['mobile'],how='left')
        dfin = dfin.drop_duplicates()

        if not dfin.empty:    
            dfin['campaign'] = campaign
            dfin['is_active'] = 1
            dfin['to_check']=1
            
            driver,sheeter = apiconnect()
            dftemp = sheetsToDf(sheeter,spreadsheet_id='1HAieTvA6iNtN_tb2jrAwHqHySDEALGoVLPi7rwsYwdg',
                                    sh_name='SMSIDS')
            
            dfin = pd.concat([dftemp,dfin],join='outer',sort=False)
            dfin = dfin.reset_index(drop=True)
#             print(dfin.dtypes)
#             print(dfin)
            
            driver,sheeter = apiconnect()
            sheet_id = dftoSheetsfast(driver,sheeter,dfin,
                                    sp_nam_id='1HAieTvA6iNtN_tb2jrAwHqHySDEALGoVLPi7rwsYwdg',sh_name = 'SMSIDS')
            print('output written')
