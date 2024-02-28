#!/usr/bin/python
# -*- coding: UTF-8 -*-

import paho.mqtt.client as mqtt
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb

import paho.mqtt.publish as publish
import json
import os
import datetime

# topic = sys.argv[1]

#源码中只需要知道   ip + 端口 + 订阅的主题
HOST ="10.114.26.111"
# HOST ="10.105.46.198"
PORT =1883
topic='HIDAS_PVA_msg'
# topic='HIDAS_process_check_app_cmd'


'''
The callback for when the client receives a CONNACK response from the server
客户端接收到服务器端的确认连接请求时，回调on_connect服务端发送CONNACK报文响应从客户端收到的CONNECT报文。
服务端发送给客户端的第一个报文必须是CONNACK [MQTT-3.2.0-1].
'''
def on_connect(client,userdata, flags,rc):
    print("Connected with result code" + str(rc))
    '''
    Subscribing in on_connect() means that if we lose the connection 
    and  reconnect then subscriptions wil be renewed(恢复、续订).'''
    # client.subscribe("HIDAS_process_check_app_cmd") # 订阅主题
    client.subscribe(topic) # 订阅主题

'''
The callback for when a PUBLISH message is received from the server.
客户端接收到服务器向其传输的消息时，
回调on_messagePUBLISH控制报文是指从客户端向服务端或者服务端向客户端传输一个应用消息。
'''
def on_message(client,userdata,msg):

    # 接收到消息时的回调函数
    json_data = json.loads(msg.payload)  # 解析JSON数据
    # print json_data


    # print(msg.topic+ msg.payload.decode("utf-8"))
    line = msg.payload.decode("unicode_escape")
    # line = str(line)
    # print type(line)
    print line

    try:
        msg_dict = {}
        msg_dict = eval(line)

        # 定义 MQTT 服务器的地址和端口号
        mqtt_server = "10.114.17.165"
        mqtt_port = 1883
        # 定义要发布的主题和消息
        topic = "HIDAS_PVA_msg_for_it"

        message = json.dumps(msg_dict)
        print message
        # 发布消息
        publish.single(topic, message, hostname=mqtt_server, port=mqtt_port)
        print '@@ send to mqtt for it @@'


    except Exception:
        print 'send to mqtt for it NG!!!'
        print 'send to mqtt for it NG!!!'
        print 'send to mqtt for it NG!!!'
        pass

    # send to database

    json_data_2 = {}

    json_key = "sender"
    if json_key in json_data :
        json_data_2['sender'] = json_data[json_key]
        del json_data['sender']

    json_key = "bay_no"
    if json_key in json_data :
        json_data_2['bay_no'] = json_data[json_key]
        del json_data['bay_no'] 

    json_key = "computer_name"
    if json_key in json_data :
        json_data_2['computer_name'] = json_data[json_key]
        del json_data['computer_name']

    json_key = "create_time"
    if json_key in json_data :
        json_data_2['create_time'] = json_data[json_key]
        del json_data['create_time']

    json_key = "msg_type"
    if json_key in json_data :
        json_data_2['msg_type'] = json_data[json_key]
        del json_data['msg_type']

    json_key = "Date"
    if json_key in json_data :
        json_data_2['Date'] = json_data[json_key]
        del json_data['Date']

    json_key = "Time"
    if json_key in json_data :
        json_data_2['Time'] = json_data[json_key]
        del json_data['Time']

    json_key = "User"
    if json_key in json_data :
        json_data_2['User'] = json_data[json_key]
        del json_data['User']

    json_key = "Event"
    if json_key in json_data :
        json_data_2['Event'] = json_data[json_key]
        del json_data['Event']

    json_key = "Mode"
    if json_key in json_data :
        json_data_2['Mode'] = json_data[json_key]
        del json_data['Mode']

    json_key = "Part_#_on_Pallet"
    if json_key in json_data :
        json_data_2['Part_on_Pallet'] = json_data[json_key]
        del json_data['Part_#_on_Pallet']

    json_key = "Program_Name"
    if json_key in json_data :
        json_data_2['product_name'] = json_data[json_key]
        del json_data['Program_Name']

    json_key = "Cycle"
    if json_key in json_data :
        json_data_2['Cycle'] = json_data[json_key].replace(' ','')
        del json_data['Cycle']

    json_key = "Barcode"
    if json_key in json_data :
        json_data_2['Barcode'] = json_data[json_key]
        del json_data['Barcode']

    json_key = "Takt_Time_"
    if json_key in json_data :
        json_data_2['Takt_Time'] = json_data[json_key].replace(' ','')
        del json_data['Takt_Time_']

    json_key = "Cycle_Time_"
    if json_key in json_data :
        json_data_2['Cycle_Time'] = json_data[json_key].replace(' ','')
        del json_data['Cycle_Time_']

    json_key = "HD1_ATOM_Enable"
    if json_key in json_data :
        json_data_2['HD1_ATOM_Enable'] = json_data[json_key].replace(' ','')
        del json_data['HD1_ATOM_Enable']

    json_key = "HD1_ATOM_Pressure"
    if json_key in json_data :
        json_data_2['HD1_ATOM_Pressure'] = json_data[json_key].replace(' ','')
        del json_data['HD1_ATOM_Pressure']

    json_key = "HD1_ATOM_Upper_limit"
    if json_key in json_data :
        json_data_2['HD1_ATOM_Upper_limit'] = json_data[json_key].replace(' ','')
        del json_data['HD1_ATOM_Upper_limit']

    json_key = "HD1_ATOM_Lower_Limit"
    if json_key in json_data :
        json_data_2['HD1_ATOM_Lower_Limit'] = json_data[json_key].replace(' ','')
        del json_data['HD1_ATOM_Lower_Limit']

    json_key = "HD2_ATOM_Enable"
    if json_key in json_data :
        json_data_2['HD2_ATOM_Enable'] = json_data[json_key].replace(' ','')
        del json_data['HD2_ATOM_Enable']

    json_key = "HD2_ATOM_Pressure"
    if json_key in json_data :
        json_data_2['HD2_ATOM_Pressure'] = json_data[json_key].replace(' ','')
        del json_data['HD2_ATOM_Pressure']

    json_key = "HD2_ATOM_Upper_limit"
    if json_key in json_data :
        json_data_2['HD2_ATOM_Upper_limit'] = json_data[json_key].replace(' ','')
        del json_data['HD2_ATOM_Upper_limit']

    json_key = "HD2_ATOM_Lower_Limit"
    if json_key in json_data :
        json_data_2['HD2_ATOM_Lower_Limit'] = json_data[json_key].replace(' ','')
        del json_data['HD2_ATOM_Lower_Limit']

    json_key = "Mat_Tank_Enable"
    if json_key in json_data :
        json_data_2['Mat_Tank_Enable'] = json_data[json_key].replace(' ','')
        del json_data['Mat_Tank_Enable']

    json_key = "Mat_Tank_Pressure"
    if json_key in json_data :
        json_data_2['Mat_Tank_Pressure'] = json_data[json_key].replace(' ','')
        del json_data['Mat_Tank_Pressure']

    json_key = "Mat_Tank_Upper_Limit"
    if json_key in json_data :
        json_data_2['Mat_Tank_Upper_Limit'] = json_data[json_key].replace(' ','')
        del json_data['Mat_Tank_Upper_Limit']

    json_key = "Mat_Tank_Lower_Limit"
    if json_key in json_data :
        json_data_2['Mat_Tank_Lower_Limit'] = json_data[json_key].replace(' ','')
        del json_data['Mat_Tank_Lower_Limit']

    json_key = "Scale_Enable"
    if json_key in json_data :
        json_data_2['Scale_Enable'] = json_data[json_key].replace(' ','')
        del json_data['Scale_Enable']

    json_key = "Scale_Weight"
    if json_key in json_data :
        json_data_2['Scale_Weight'] = json_data[json_key].replace(' ','')
        del json_data['Scale_Weight']

    json_key = "Low_Level_Setting"
    if json_key in json_data :
        json_data_2['Low_Level_Setting'] = json_data[json_key].replace(' ','')
        del json_data['Low_Level_Setting']

    json_key = "Empty_level_Setting"
    if json_key in json_data :
        json_data_2['Empty_level_Setting'] = json_data[json_key].replace(' ','')
        del json_data['Empty_level_Setting']

    json_key = "Light_Tower"
    if json_key in json_data :
        json_data_2['Light_Tower'] = json_data[json_key].replace(' ','')
        del json_data['Light_Tower']


    # for var_dict_key in json_data:
    #     var_dict_key = str(var_dict_key)
    #     try:
    #         sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'ods_axxon_glue_mqtt' AND column_name = '" + var_dict_key + "';"
    #         # print sql
    #         db = MySQLdb.connect('cnhuam0dp06','valeo','vdb#20230608','valeo_db',4001)
    #         cursor = db.cursor()
    #         cursor.execute(sql)
    #         data = cursor.fetchone()
    #         db.close()

    #         if 'None' in str(data) :
    #             print 'insert'
    #             sql = 'ALTER TABLE ods_axxon_glue_mqtt ADD COLUMN ' + var_dict_key + ' VARCHAR(64) DEFAULT NULL;'
    #             print sql
    #             db = MySQLdb.connect('cnhuam0dp06','valeo','vdb#20230608','valeo_db',4001)
    #             # db = MySQLdb.connect('127.0.0.1','root','barcode','valeo_db',3306)
    #             # db = MySQLdb.connect('120.77.249.45','root','barcode','valeo_db',3306)
    #             cursor = db.cursor()

    #             # 将字段的value转化为元祖存入
    #             cursor.execute(sql)
    #             db.commit()
    #             db.close()

    #     except Exception:
    #         print 'Database insert COLUMN NG!!!'
    #         print 'Database insert COLUMN NG!!!'
    #         print 'Database insert COLUMN NG!!!'
    #         pass



    # print json_data_2
    try:
        db = MySQLdb.connect('cnhuam0dp06','valeo','vdb#20230608','valeo_db',4001,charset='utf8')
        # db = MySQLdb.connect('127.0.0.1','root','barcode','valeo_db',3306)
        # db = MySQLdb.connect('120.77.249.45','root','barcode','valeo_db',3306)

    except Exception:
        print 'Database NG!!!'
        print 'Database NG!!!'
        print 'Database NG!!!'
        pass

    cursor = db.cursor()

    #表名
    table = "ods_pva_mqtt"
    # 列的字段
    keys = ', '.join(json_data_2.keys())
    # 行字段
    values = ', '.join(['%s']*len(json_data_2))
    sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)
    # print sql
    # 将字段的value转化为元祖存入
    cursor.execute(sql, tuple(json_data_2.values()))
    db.commit()
    db.close()
    print '@@ ods_pva_mqtt @@'




def client_loop():
    '''
    注意，client_id是必须的，并且是唯一的。否则可能会出现如下错误
    [WinError 10054] 远程主机强迫关闭了一个现有的连接
    '''

    client_id = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    print client_id
    client = mqtt.Client(client_id) # Client_id 不能重复，所以使用当前时间
    client.username_pw_set("admin","password") # 必须设置，否则会返回 /Connected with result code 4/
    client.on_connect = on_connect
    client.on_message = on_message

    '''
    拥塞回调:处理网络流量，调度回调和重连接。
    Blocking call that processes network traffic, 
    dispatches callbacks and handles reconnecting.
    Other loop*() functions are available that give a threaded interface and amanual- interface...I
    '''
    try:
        client.connect(HOST,PORT,60)
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()

if __name__ == '__main__':
    print("rx:")
    client_loop()


# import paho.mqtt.client as mqtt


# def on_message(client, userdata, message):
#   print("Received message: " + str(message.payload.decode("utf-8")))

# client = mqtt.Client()
# client.on_message = on_message
# client.connect("10.114.26.111", 1883)
# client.subscribe("HIDAS_process_check_app_msg")
# client.loop_start()
