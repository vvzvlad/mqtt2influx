#!/usr/bin/env -S python3 -u

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

import time

import os
import json
import sys
import random

def test_number(value):
  #return value.isdigit()
  try:
    float(value)
    return True
  except:
    return False

def test_json(value):
    try:
        json_object = json.loads(value)
    except:
      return False
    return True

def upload_to_influx(topic, payload):
    json_body = [ { "measurement": topic, "fields": { "value": float(payload) } } ]
    influx_client.write_points(json_body)


def parse_json(json_string, topic):
  json_object = json.loads(json_string)
  for key in json_object:
    topic_m = topic + '/' + key
    value = json_object[key]
    #print('JN: ' + topic_m + '=' + str(value))
    parse_message(topic_m, value)

def parse_message(topic, payload):
  is_number = test_number(payload)
  is_json = test_json(payload)

  #print(is_number, is_json, topic, payload)

  if is_number == True:
    print('N: '+ topic +'=' + str(payload))
    upload_to_influx(topic, payload)
  elif is_json == True and is_number == False:
    #print('J: '+ topic +'=' + payload)
    parse_json(payload, topic)
  elif is_number == False and is_json == False:
    print('T: '+ topic +'=' + str(payload))
    #upload_to_influx(topic, payload)


def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("#")
  client.publish("mqtt2influx/status", payload="mqtt2influx daemon started", qos=0, retain=False)


def on_message(client, userdata, msg):
  if msg.retain == False:
    parse_message(msg.topic, msg.payload.decode("utf-8"))
  #print(msg.topic + ": " + msg.payload.decode("utf-8"))

def main():
  global influx_client
  influx_client = InfluxDBClient('192.168.88.111', 8086, 'root', 'root', 'smarthome')
  influx_client.create_database('smarthome')
  counter = 0
  period = 60
  client = mqtt.Client()
  client.on_connect = on_connect
  client.on_message = on_message
  client.connect("192.168.88.111", 1883, 60)
  time.sleep(5)
  client.loop_start()
  while True:
    uptime = counter * period
    client.publish("mqtt2influx/status/uptime", str(uptime), qos=0, retain=False)
    time.sleep(period)
    counter = counter + 1
  client.loop_stop()



if __name__ == "__main__":
  main()
