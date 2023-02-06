# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 17:31:16 2022

@author: Hoai-Nam
"""

import pandas as pd
import requests
import warnings
import time
# from time import sleep
from bs4 import BeautifulSoup
import json
import os
import datetime
import numpy as np
import pymongo
from pymongo import MongoClient
import re


# =============================================================================

# MongoDB Database & Collection

Database="WiFi_Client_Data"
Collections="Controller_4"

# =============================================================================

# Aruba API account & password

account = 'apiUser'
password = 'x564#kdHrtNb563abcde'

# =============================================================================


Controller_url='https://140.118.151.248:4343'

# Avoid warning

warnings.filterwarnings('ignore') 
path = 'data.txt'
# =============================================================================

# auto login and get cookie

url = Controller_url+'/screens/wms/wms.login'
headers = {'Content-Type': 'text/html','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
chartData = 'opcode=login&url=%2Flogin.html&needxml=0&uid='+account+'&passwd='+password
res_data = requests.post(url, verify=False, headers = headers, data = chartData.encode('utf-8'))
cookieStr = res_data.cookies['SESSION']

# =============================================================================

# Retrieve and parse AP data


url = Controller_url+'/screens/cmnutil/execUiQuery.xml'
headers = {'Content-Type': 'text/plain'}
cookie = {"SESSION":cookieStr}
payloadData = 'query=<aruba_queries><query><qname>backend-observer-mon_bssid-17</qname><type>list</type><list_query><device_type>mon_bssid</device_type><requested_columns>mon_ap mon_bssid mon_radio_phy_type mon_ssid mon_radio_band mon_ap_current_channel mon_ht_sec_channel mon_sta_count mon_ap_classification mon_ap_match_conf_level mon_ap_encr mon_ap_encr_auth mon_ap_encr_cipher mon_ap_is_dos mon_ap_type mon_ap_status mon_is_ibss mon_ap_create_time mon_ap_match_type mon_ap_match_method mon_ap_match_name mon_ap_match_time wms_event_count</requested_columns><sort_by_field>mon_ssid</sort_by_field><sort_order>desc</sort_order><pagination><start_row>0</start_row><num_rows>1400</num_rows></pagination></list_query><filter><global_operator>and</global_operator><filter_list><filter_item_entry><field_name>mon_ap_status</field_name><comp_operator>equals</comp_operator><value><![CDATA[1]]></value></filter_item_entry></filter_list></filter></query></aruba_queries>&UIDARUBA='+cookieStr

res = requests.post(url, verify=False, headers = headers, cookies = cookie, data = payloadData.encode('utf-8'))

soup = BeautifulSoup(res.text, 'html.parser')
header_tags = soup.find_all('header')
row_tags=soup.find_all('row')

# =============================================================================

# Rearrange DataFrame

df=pd.DataFrame()
index=0

row_tags[0]
for values in row_tags:
    
    data=values.find_all('value')
    data_total=[]
    
    time_stamp =int(time.time())
    struct_time = time.localtime(time_stamp) 
    timeString = time.strftime("%Y-%m-%d-%H-%M", struct_time) 
    data_total.append(time_stamp)

    for i in range(len(data)):

        data_total.append(data[i].text)
        
    index+=1
    df[index]=data_total

# =============================================================================

# Add header to dataframe

for values in header_tags:
    Client_Data=[] 
    Client_Data.append('time_stamp')
    column_name=values.find_all('column_name')
    for i in range(len(column_name)) :
        Client_Data.append(column_name[i].text)


df.index=Client_Data
df=df.T
df.reset_index(drop=True, inplace=True)


# =============================================================================

# Add datetime (GMT +8) and timestamp

import datetime
from datetime import timedelta

ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
ts = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fz")
ts
n = 8
# Subtract 8 hours from datetime object
ts = ts - timedelta(hours=n)
ts_tw_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
ts_tw = datetime.datetime.now()

data_json = json.loads(df.to_json(orient='records'))

for i in range(len(data_json)):
    try:
        data_json[i]['mon_ssid'] = data_json[i]['mon_ssid'].encode('latin-1').decode('utf-8')
        data_json[i]['mon_radio_band'] = int(data_json[i]['mon_radio_band'])
        data_json[i]['mon_ap_current_channel'] = int(data_json[i]['mon_ap_current_channel'])
        data_json[i]['mon_ht_sec_channel'] = int(data_json[i]['mon_ht_sec_channel'])
        data_json[i]['mon_sta_count'] = int(data_json[i]['mon_sta_count'])
        data_json[i]['mon_ap_classification'] = int(data_json[i]['mon_ap_classification'])
        data_json[i]['mon_ap_match_conf_level'] = int(data_json[i]['mon_ap_match_conf_level'])
        data_json[i]['mon_ap_encr'] = int(data_json[i]['mon_ap_encr'])
        data_json[i]['mon_ap_encr_auth']= int(data_json[i]['mon_ap_encr_auth'])
        data_json[i]['mon_ap_encr_cipher']= int(data_json[i]['mon_ap_encr_cipher'])
        data_json[i]['mon_ap_is_dos']= int(data_json[i]['mon_ap_is_dos'])
        data_json[i]['mon_ap_type']= int(data_json[i]['mon_ap_type'])
        data_json[i]['mon_ap_status']= int(data_json[i]['mon_ap_status'])
        data_json[i]['mon_is_ibss']= int(data_json[i]['mon_is_ibss'])
        data_json[i]['mon_ap_create_time']= int(data_json[i]['mon_ap_create_time'])
        data_json[i]['mon_ap_match_type']= int(data_json[i]['mon_ap_match_type'])
        data_json[i]['mon_ap_match_method']= int(data_json[i]['mon_ap_match_method'])
        data_json[i]['mon_ap_match_name']= int(data_json[i]['mon_ap_match_name'])
        data_json[i]['mon_ap_match_time']= int(data_json[i]['mon_ap_match_time'])
        data_json[i]['wms_event_count']= int(data_json[i]['wms_event_count'])
    except Exception:
        pass
    data_json[i]['ts'] = ts 
    data_json[i]['DatetimeStr'] = ts_tw_str
    data_json[i]['Datetime'] = ts_tw
data_json[1]

# =============================================================================

# Store json data to MongoDB

client = MongoClient("140.118.70.40",27017)
db = client['WiFi_Interference_Data']
col=db["Controller_4"]
col.insert_many(data_json)

from bson.objectid import ObjectId
from datetime import datetime, timedelta

previous_day = datetime.now() - timedelta(days=1) 

col.delete_many({"Datetime": {"$lt": previous_day}})
    
print('Done!')
