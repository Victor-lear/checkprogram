# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 17:36:40 2022

@author: Administrator
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
payloadData = 'query=<aruba_queries><query><qname>backend-observer-radio-8</qname><type>list</type><list_query><device_type>radio</device_type><requested_columns>ap_eth_mac_address mesh_portal_ap_mac mesh_uplink_age ap_name role radio_band radio_bssid radio_ht_phy_type channel_str channel_5_ghz channel_2_4_ghz radio_generic_mode radio_mode radio_number pcap_id pcap_state pcap_target_ip pcap_target_port eirp_10x max_eirp noise_floor arm_ch_qual total_moves successful_moves sta_count current_channel_utilization rx_time tx_time channel_interference channel_free channel_busy avg_data_rate tx_avg_data_rate rx_avg_data_rate tx_frames_transmitted tx_frames_dropped tx_bytes_transmitted tx_bytes_dropped tx_time_transmitted tx_time_dropped tx_data_transmitted tx_data_dropped tx_data_retried tx_data_transmitted_retried tx_data_bytes_transmitted tx_data_bytes_dropped tx_bcast_data tx_mcast_data tx_ucast_data tx_time_data_transmitted tx_time_data_dropped tx_mgmt rx_promisc_good rx_promisc_bad rx_frames rx_frames_others rx_promisc_bytes rx_bytes rx_bytes_others rx_promisc_data rx_data rx_data_others rx_promisc_data_bytes rx_data_bytes rx_data_bytes_others rx_data_retried rx_bcast_data rx_mcast_data rx_ucast_data tx_data_frame_rate_dist rx_data_frame_rate_dist tx_data_bytes_rate_dist rx_data_bytes_rate_dist total_data_throughput tx_data_throughput rx_data_throughput total_bcast_data total_mcast_data total_ucast_data total_data_frames total_data_bytes total_data_type_dist tx_data_type_dist rx_data_type_dist vap_count ap_quality mesh_rssi mesh_tx_goodput mesh_rx_goodput mesh_tx_throughput mesh_rx_throughput mesh_tx_success mesh_tx_retry mesh_tx_drop mesh_rx_success mesh_rx_retry</requested_columns><sort_by_field>ap_name</sort_by_field><sort_order>asc</sort_order><pagination><start_row>0</start_row><num_rows>200</num_rows></pagination></list_query></query></aruba_queries>&UIDARUBA='+cookieStr

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
df2 = df

import re

df2['sta_count_all'] = df2['sta_count']
for i in range(len(df2)):
    df2['rx_time'][i] = int(re.findall("([0-9]+)\/", df2['rx_time'][i])[0])/60000
    df2['tx_time'][i] = int(re.findall("([0-9]+)\/", df2['tx_time'][i])[0])/60000
    df2['channel_interference'][i] = int(re.findall("([0-9]+)\/", df2['channel_interference'][i])[0])/60000
    df2['channel_free'][i] = int(re.findall("([0-9]+)\/", df2['channel_free'][i])[0])/60000
    df2['radio_band'][i] = int(df2['radio_band'][i])
    df2['noise_floor'][i] = int(df2['noise_floor'][i])
    df2['sta_count'][i] = int(df2['sta_count'][i])
    # Add total client number
    for i in range(len(df2) - 1):
        if df2['ap_name'][i] == df2['ap_name'][i+1]:
            df2['sta_count_all'][i] = int(df2['sta_count'][i]) + int(df2['sta_count'][i+1])
            df2['sta_count_all'][i+1] = df2['sta_count_all'][i]

# df2.head(40)

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

data_json = json.loads(df2.to_json(orient='records'))

for i in range(len(data_json)):
    data_json[i]['ts'] = ts 
    data_json[i]['DatetimeStr'] = ts_tw_str
    data_json[i]['Datetime'] = ts_tw

# =============================================================================

# Store json data to MongoDB
from bson.objectid import ObjectId
from datetime import datetime, timedelta

previous_day = datetime.now() - timedelta(minutes=10) 

client = MongoClient("140.118.70.40",27017)
db = client['WiFi_Dashboard_Data']
col=db["AP_List"]
col.delete_many({"Datetime": {"$lt": previous_day}})
col.insert_many(data_json)