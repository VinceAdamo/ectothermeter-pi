import time
import board
import adafruit_dht
import datetime
import paho.mqtt.client as paho
import json
import os
from sys import exit
from dotenv import load_dotenv


load_dotenv()

MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
CA_PATH = os.getenv('CA_PATH')

def get_serial():
  cpuserial = "0000000000000000"
  try:
    f = open('/proc/cpuinfo','r')
    for line in f:
      if line[0:6]=='Serial':
        cpuserial = line[10:26]
    f.close()
  except:
    cpuserial = "ERROR000000000"

  return cpuserial
  
def on_publish(client, userdata, mid, reason_codes, properties):
    print("Message Published")
    
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Failed to connect: {reason_code}")
    else:
        print("Connection Successful")
        client.connected_flag=True;
        
def clean_up(client, dht_device):
    print("Disconnecting DHT device")
    dht_device.exit()
    print("Stopping loop")
    client.loop_stop()
    print("Disconnecting client")
    client.disconnect()

dht_device = adafruit_dht.DHT11(board.D4)

serial = get_serial()

topic = f"devices/{serial}/readings"

client = paho.Client(paho.CallbackAPIVersion.VERSION2, serial)  

client.connected_flag=False

client.on_publish = on_publish  
client.on_connect = on_connect

client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

client.tls_set(
    cert_reqs=paho.ssl.CERT_REQUIRED,
    tls_version=paho.ssl.PROTOCOL_TLSv1_2,
    ca_certs=CA_PATH
)
                          
client.connect(MQTT_HOST, MQTT_PORT)

client.loop_start()

print("Waiting for connection...")
time.sleep(5.0)

if not client.connected_flag:
    print("Unable to connect to mqtt. Exiting...")
    exit()

try:
    while True:
        try:
            temperature_c = dht_device.temperature
            temperature_f = temperature_c * (9 / 5) + 32
            
            humidity = dht_device.humidity
            
            timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
            
            print(
                "DeviceId: {}   Temp: {:.1f} F / {:.1f} C    Humidity: {}%   Timestamp: {}".format(
                serial, temperature_f, temperature_c, humidity, timestamp
            ))
            
            data = {
                'temperature': temperature_f, 
                'timestamp': timestamp, 
                'humidity': humidity, 
                'serialNumber': serial 
            }
            
            payload = json.dumps(data)
            
            ret = client.publish(topic, payload)  
            
            status = ret[0]
            
            if status == 0:
                print(f"Message successfully sent to topic {topic}")
            else:
                print(f"Failed to send message to topic {topic}")
        except RuntimeError as error:
            print(error.args[0])
        except Exception as error:
            raise error
        
        time.sleep(60.0)
except KeyboardInterrupt:
    print("Keyboard Interrupt Received")
    clean_up(client, dht_device)
except Exception as error:
    print(error)
    clean_up(client, dht_device)
