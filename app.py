#!/usr/bin/env -S python3 -u

import paho.mqtt.client as mqtt
import time

import os
import json
import sys
import random


def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("#")
  client.publish("testmqtt/status", payload="testmqtt daemon started", qos=0, retain=False)


def on_message(client, userdata, msg):
  print(msg.topic + ": " + msg.payload.decode("utf-8"))



def main():
  counter = 0
  period = 10
  client = mqtt.Client()
  client.on_connect = on_connect
  client.on_message = on_message
  client.connect("192.168.88.111", 1883, 60)
  time.sleep(5)
  client.loop_start()
  while True:
    uptime = counter * period
    client.publish("testmqtt/status/uptime", str(uptime), qos=0, retain=False)
    time.sleep(period)
    counter = counter + 1
  client.loop_stop()



if __name__ == "__main__":
  main()
