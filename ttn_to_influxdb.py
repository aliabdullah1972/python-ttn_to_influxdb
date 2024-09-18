import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS  # Correct import

# TTN MQTT settings
mqtt_broker = "eu1.cloud.thethings.network"  # Update with your region (e.g., eu1, nam1, etc.)
app_id = "aliapp1"                   # Replace with your TTN application ID
dev_id = "ali-dev1"                 # Replace with your TTN device ID
access_key = "NNSXS.MIC7JCG5B6MQOQ5FGOQ2RIB7QZAITMX46SJSPLA.LX4UTXI7E4SYIYW4Y2C3AC53NDVPZ53HIVBMU6EOB5GMC3P6GKXA"           # Replace with your TTN API key

# InfluxDB settings
influx_url = "http://192.168.0.120:8086"         # InfluxDB URL, adjust if using InfluxDB Cloud
token = "Y-X81WNGBDZrCLwr-L0XFm7hpPdGE5JpSMv9IfZx3fE3OUSA2vxXdF2e4RpItYal9uPuhNiq7gPov6BZvnswbA=="                # InfluxDB authentication token
org = "KIST"                             # InfluxDB organization name
bucket = "ttn_windows"                       # InfluxDB bucket name

# Initialize InfluxDB client
client = InfluxDBClient(url=influx_url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# MQTT callback when connection is established
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to TTN MQTT broker successfully!")
        client.subscribe(f"v3/{app_id}@ttn/devices/{dev_id}/up")
    else:
        print(f"Failed to connect to TTN, return code {rc}")

# MQTT callback when a message is received
def on_message(client, userdata, msg):
    try:
        ttn_message = json.loads(msg.payload.decode())
        device_id = ttn_message['end_device_ids']['device_id']
        decoded_payload = ttn_message['uplink_message']['decoded_payload']
        
        # Print the entire payload for debugging
        print(f"Decoded payload: {decoded_payload}")
        
        # Convert to float to avoid type conflicts
        humidity = float(decoded_payload.get('humidity', 0.0))
        pressure = float(decoded_payload.get('pressure', 0.0))
        temperature = float(decoded_payload.get('temperature', 0.0))
        
        print(f"Device {device_id}: Temperature: {temperature}°C, Humidity: {humidity}%, Pressure: {pressure} hPa")
        
        if temperature is not None and humidity is not None and pressure is not None:
            point = Point("sensor_data") \
                        .tag("device", device_id) \
                        .field("temperature", temperature) \
                        .field("humidity", humidity) \
                        .field("pressure", pressure)
            try:
                write_api.write(bucket=bucket, org=org, record=point)
                print(f"Data written to InfluxDB: {temperature}°C, {humidity}%, {pressure} hPa")
            except Exception as e:
                print(f"Error writing data to InfluxDB: {e}")
        else:
            print("Incomplete data, nothing written to InfluxDB.")
    except Exception as e:
        print(f"Error processing message: {e}")

# MQTT client setup
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(f"{app_id}@ttn", access_key)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to TTN MQTT broker
mqtt_client.connect(mqtt_broker, 1883, 60)
mqtt_client.loop_forever()
