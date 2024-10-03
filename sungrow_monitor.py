"""
Script for connecting to a Sungrow inverter via WebSocket, retrieving data,
formatting it, and publishing it to an MQTT broker.

Dependencies:
    - Paho MQTT client library (https://pypi.org/project/paho-mqtt/)
    - sungrow_websocket module (https://pypi.org/project/sungrow-websocket/)
    - sdnotify (https://pypi.org/project/sdnotify/)


The script establishes a connection to the Sungrow inverter via WebSocket,
retrieves the data, and constructs a payload dictionary containing sensor
data such as sensor ID, time collected, and various measurements. It then
formats the payload into a string, ensuring it adheres to the JSON format
required by the destination system, such as Telegraf for InfluxDB. The
formatted payload is published to an MQTT broker specified by the MQTT_HOST
and MQTT_PORT variables.

Author: Ben Johns (bjohns@naturalnetworks.net)

"""
import time
import paho.mqtt.client as mqtt
from sungrow_websocket import SungrowWebsocket

import sdnotify

# Initialize sdnotify
notifier = sdnotify.SystemdNotifier()

# Sungrow inverter details
SUNGROW_IP = 'sungrow.home.arpa'

# MQTT details
SENSOR_ID = "rpizero.home.arpa"
MQTT_HOST = 'nas.home.arpa'
MQTT_PORT = 1883
MQTT_TOPIC = 'home/sungrow'

def receive_and_publish():
    """
    Connects to the Sungrow inverter via WebSocket, retrieves data,
    formats it, and publishes it to an MQTT broker.

    The function establishes a connection to the Sungrow inverter via
    WebSocket, retrieves the data, and constructs a payload dictionary
    containing sensor data such as sensor ID, time collected, and various
    measurements. It then formats the payload into a string, ensuring it
    adheres to the JSON format required by the destination system, such
    as Telegraf for InfluxDB. The formatted payload is published to an
    MQTT broker specified by the MQTT_HOST and MQTT_PORT variables.

    Parameters:
        None

    Returns:
        None
    """
    sg = SungrowWebsocket(SUNGROW_IP)
    try:
        data = sg.get_data()

        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

        # Publish each data item to MQTT
        payload = {}
        payload['sensorID'] = SENSOR_ID
        payload['timecollected'] = int(time.time())  # epoch seconds
        for item in data.values():
            value = item.value
            desc = item.desc
            if value == "--":
                value = int(0.0)
            payload[desc] = value

        # Ensure json format
        payload_str = "{" + ", ".join(
            f'"{key.replace("(", "").replace(")", "").replace(" ", "_").strip()}": "{str(value).strip()}"' if not isinstance(value, int) and not value.replace(".", "", 1).isdigit() else
            f'"{key.replace("(", "").replace(")", "").replace(" ", "_").strip()}": {value}'
            for key, value in payload.items()
        ) + "}"

        mqtt_client.publish(MQTT_TOPIC, payload_str)

        mqtt_client.disconnect()

    except Exception as e:
        print("An error occurred:")
        print(str(e))
        return

def main(interval_seconds):
    """
    Main function of the script.

    The main function continuously calls the receive_and_publish function
    with a specified interval to establish a connection to the Sungrow
    inverter via WebSocket, retrieve data, format it, and publish it to
    an MQTT broker.

    Parameters:
        interval_seconds (int): Interval in seconds between each data publishing.

    Returns:
        None
    """
    while True:
        receive_and_publish()
        time.sleep(interval_seconds)

        # Trigger the watchdog
        notifier.notify("WATCHDOG=1")

if __name__ == "__main__":
    # Set the interval in seconds
    interval_seconds = 5
    main(interval_seconds)
