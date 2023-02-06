# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 15:28:21 2022

@author: GE75
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



# =============================================================================
# Database /Collection 資訊

Database="WiFi_Client_Data"
Collections="Controller_4"
# =============================================================================


# =============================================================================
# 輸入登入 Aruba 帳號密碼
account = 'apiUser'
password = 'x564#kdHrtNb563abcde'

# =============================================================================


Controller_url='https://140.118.151.248:4343'



def OutputCSV(csv_output) : 
    time_stamp =int(time.time())# 設定timeStamp
    struct_time = time.localtime(time_stamp) # 轉成時間元組
    timeString = time.strftime("%Y-%m-%d-%H-%M-%S", struct_time) # 轉成字串
    print(timeString)
    Result ='D:\\WiFi資料(日後刪除)'+'\\'+'WiFi_Data_client'+timeString+'.csv'
    csv_output.to_csv( Result  , index=False )



def check_db() :
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')
     
    dblist = myclient.list_database_names()
    # dblist = myclient.database_names() 
    if "runoobdb" in dblist:
      print("数据库已存在！")




def login(account,password,Controller_url):

    warnings.filterwarnings('ignore') #忽略warning
    path = 'data.txt'
    

    
    # auto login and get cookie
    url = Controller_url+'/screens/wms/wms.login'
    headers = {'Content-Type': 'text/html','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'}
    chartData = 'opcode=login&url=%2Flogin.html&needxml=0&uid='+account+'&passwd='+password
    res_data = requests.post(url, verify=False, headers = headers, data = chartData.encode('utf-8'))
    print("cookies.SESSION:",res_data.cookies['SESSION'])
    cookieStr = res_data.cookies['SESSION']
#cookieStr = '35ee1a97-06d7-41c1-a10b-7ee8e5aba691'
    return cookieStr




# =============================================================================
# 登入Controller
# =============================================================================
cookieStr=login(account,password,Controller_url)

start = time.time()

# =============================================================================
# 檢查Ren
# =============================================================================
check_db()



print("Start Collect data from "+Collections)


url = Controller_url+'/screens/cmnutil/execUiQuery.xml'
headers = {'Content-Type': 'text/plain'}
cookie = {"SESSION":cookieStr}
payloadData = 'query=<aruba_queries><query><qname>backend-observer-sta-17</qname><type>list</type><list_query><device_type>sta</device_type><requested_columns>sta_mac_address client_ht_phy_type openflow_state client_ip_address client_user_name client_dev_type client_ap_location client_conn_port client_conn_type client_timestamp client_role_name client_active_uac client_standby_uac ap_cluster_name client_health total_moves successful_moves steer_capability ssid ap_name channel channel_str channel_busy tx_time rx_time channel_free channel_interference current_channel_utilization radio_band bssid speed max_negotiated_rate noise_floor radio_ht_phy_type snr total_data_frames total_data_bytes avg_data_rate tx_avg_data_rate rx_avg_data_rate tx_frames_transmitted tx_frames_dropped tx_bytes_transmitted tx_bytes_dropped tx_time_transmitted tx_time_dropped tx_data_transmitted tx_data_dropped tx_data_retried tx_data_transmitted_retried tx_data_bytes_transmitted tx_abs_data_bytes tx_data_bytes_dropped tx_time_data_transmitted tx_time_data_dropped tx_mgmt rx_frames rx_bytes rx_data rx_data_bytes rx_abs_data_bytes rx_data_retried tx_data_frame_rate_dist rx_data_frame_rate_dist tx_data_bytes_rate_dist rx_data_bytes_rate_dist connection_type_classification total_data_throughput tx_data_throughput rx_data_throughput client_auth_type client_auth_subtype client_encrypt_type client_fwd_mode</requested_columns><sort_by_field>client_user_name</sort_by_field><sort_order>desc</sort_order><pagination><start_row>0</start_row><num_rows>200</num_rows></pagination></list_query><filter><global_operator>and</global_operator><filter_list><filter_item_entry><field_name>client_conn_type</field_name><comp_operator>not_equals</comp_operator><value><![CDATA[0]]></value></filter_item_entry></filter_list></filter></query></aruba_queries>&UIDARUBA='+cookieStr

res = requests.post(url, verify=False, headers = headers, cookies = cookie, data = payloadData.encode('utf-8'))

soup = BeautifulSoup(res.text, 'html.parser')
header_tags = soup.find_all('header')


#df=pd.DataFrame(columns=['AP Name','Status','Provisioned','Up time','Client','Mode','Model','AP_Group','IP address','MAC address'])
df=pd.DataFrame()


# =============================================================================
# API抓取Client 資料
# =============================================================================

row_tags=soup.find_all('row')
index=0
for values in row_tags:
    
    data=values.find_all('value')
    data_total=[]
    
    time_stamp =int(time.time())# 設定timeStamp
    struct_time = time.localtime(time_stamp) # 轉成時間元組
    timeString = time.strftime("%Y-%m-%d-%H-%M", struct_time) # 轉成字串
    data_total.append(time_stamp)

    for i in range(len(data)):

        data_total.append(data[i].text)
        
    index+=1
    df[index]=data_total




# =============================================================================
# API抓取欄位名稱
# =============================================================================
for values in header_tags:
    Client_Data=[] 
    Client_Data.append('time_stamp')
    column_name=values.find_all('column_name')
    for i in range(len(column_name)) :
        #print(column_name[i].text)
        Client_Data.append(column_name[i].text)


df.index=Client_Data
df=df.T
df=df.sort_values(by=['ap_name'])
df.reset_index(drop=True, inplace=True)

# =============================================================================
# 輸出CSV檔案
# =============================================================================
#OutputCSV(df)




# =============================================================================
# 寫入Database(未完成)
# =============================================================================
client = MongoClient("localhost",27017)
db = client[Database]
col=db["Controller_4"]
print(col)
data_json = json.loads(df.to_json(orient='records'))
x=col.insert_many(data_json)
# =============================================================================
# 1分鐘執行一次
# =============================================================================
time.sleep(60 - datetime.datetime.now().second)


print("Stop Collect data from "+Collections)

