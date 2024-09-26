from dotenv import load_dotenv
load_dotenv()

import cc_lib
from cc_lib.types._device import Device
from cc_lib.types.message._envelope import EventEnvelope
from cc_lib.types.message._message import DeviceMessage
import time 
import pickle
import json
import pandas as pd 
import os 

file_path = "./Test-Data/trivial_forecast_data.pickle"

def create_sample(power, time):
    return f"""
    {{
            "ENERGY": {{
                "ApparentPower": 8, 
                "Current": 0.037, 
                "Factor": 0.52, 
                "Period": 0, 
                "Power": {power}, 
                "ReactivePower": 7, 
                "Today": 0.676, 
                "Total": 1057.112, 
                "TotalStartTime": "2022-08-07T09:30:03", 
                "Total_unit": "Kilowatt Hours", 
                "Voltage": 226, 
                "Yesterday": 1.196
            }}, 
            "Time": "{time}", 
            "Time_unit": "Timestamp (ISO)"
        }}
    """

def load_data(file_path):
    data = pd.read_pickle(file_path)

    for i in range(len(data)-1):
        current_sample = create_sample(data.iloc[i], data.index[i].isoformat())
        next_sample = create_sample(data.iloc[i+1], data.index[i+1].isoformat())
        yield current_sample, next_sample

def send_missing_power(connector_client, device, service):
    missing_power_message = """
    {
            "ENERGY": {
                "ApparentPower": 8, 
                "Current": 0.037, 
                "Factor": 0.52, 
                "Period": 0, 
                "ReactivePower": 7, 
                "Today": 0.676, 
                "Total": 1057.112, 
                "TotalStartTime": "2022-08-07T09:30:03", 
                "Total_unit": "Kilowatt Hours", 
                "Voltage": 226, 
                "Yesterday": 1.196
            }, 
            "Time": "2022-08-07T09:30:03", 
            "Time_unit": "Timestamp (ISO)"
        }
    """
    message = DeviceMessage(data=missing_power_message)
    envelope = EventEnvelope(device=device, service=service, message=message)
    connector_client.send_event(envelope)

def send_missing_energy(connector_client, device, service):
    missing_power_message = """
    {
            "ENERbbbbbGY": {
                "ApparentPower": 8, 
                "Current": 0.037, 
                "Factor": 0.52, 
                "Period": 0, 
                "ReactivePower": 7, 
                "Today": 0.676, 
                "Total": 1057.112, 
                "TotalStartTime": "2022-08-07T09:30:03", 
                "Total_unit": "Kilowatt Hours", 
                "Voltage": 226, 
                "Yesterday": 1.196
            }, 
            "Time": "2022-08-07T09:30:03", 
            "Time_unit": "Timestamp (ISO)"
        }
    """
    message = DeviceMessage(data=missing_power_message)
    envelope = EventEnvelope(device=device, service=service, message=message)
    connector_client.send_event(envelope)

def run(hub_id, hub_name, device_name, device_id, device_type_id):
    connector_client = cc_lib.client.Client()
    connector_client.connect(reconnect=True)

    device = Device(device_id, device_name, device_type_id, [])
    service = "SENSOR"

    if os.environ.get("ONLY_CREATE_DEVICE", None) == "true":
        connector_client.add_device(device)
        return

    #send_missing_energy(connector_client, device, service)
    #send_missing_power(connector_client, device, service)
    
    for current_sample, next_sample in load_data(file_path):
        now = json.loads(current_sample)['Time']
        print(f"send sample from {now}")
        message = DeviceMessage(data=current_sample)
        envelope = EventEnvelope(device=device, service=service, message=message)

        for i in enumerate([1,2,3]):
            try:
                connector_client.send_event(envelope)
                break
            except:
                time.sleep(30)
                print("Sample not sent!")

    
        wait_time = (pd.to_datetime(json.loads(next_sample)['Time']) - pd.to_datetime(now)).total_seconds()
        #time.sleep(wait_time)

if __name__ == "__main__":
    device_id = os.environ["DEVICE_ID"]
    hub_name = os.environ["HUB_NAME"]
    hub_id = os.environ.get("HUB_ID", None)

    device_name = os.environ["DEVICE_NAME"]
    #device_type_id = "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d0e" 
    device_type_id = os.environ["DEVICE_TYPE_ID"]

    run(hub_id, hub_name, device_name, device_id, device_type_id)
