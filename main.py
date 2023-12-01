from dotenv import load_dotenv
load_dotenv()

import cc_lib
from cc_lib.types._device import Device
from cc_lib.types.message._envelope import EventEnvelope
from cc_lib.types.message._message import DeviceMessage
import time 
import pickle

file_path = "./Test-Data/strange_Kühlschrank_curve.pickle"

def load_data(file_path):
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    data_stream = []
    for index in data.index:
        data_stream.append(f"""
    {{
            "ENERGY": {{
                "ApparentPower": 8, 
                "Current": 0.037, 
                "Factor": 0.52, 
                "Period": 0, 
                "Power": {data.loc[index]}, 
                "ReactivePower": 7, 
                "Today": 0.676, 
                "Total": 1057.112, 
                "TotalStartTime": "2022-08-07T09:30:03", 
                "Total_unit": "Kilowatt Hours", 
                "Voltage": 226, 
                "Yesterday": 1.196
            }}, 
            "Time": "{index.isoformat()}", 
            "Time_unit": "Timestamp (ISO)"
        }}
    """)
    return data_stream

if __name__ == "__main__":
    device_id = "anomaly-id"
    #hub_id = "urn:infai:ses:hub:14d8f5c1-e93c-4e64-9ffc-c228668859cf" # PROD
    hub_id = "urn:infai:ses:hub:0789ec9b-cbbd-4a92-906c-9278d329056f"
    hub_name = "Test"

    device_name = "Test-Anomaly-Device"
    device_type_id = "urn:infai:ses:device-type:f4bb792a-b8d3-41d6-98a8-4407b5192d0e" 

    connector_client = cc_lib.client.Client()
    connector_client.init_hub(hub_id=hub_id, hub_name=hub_name)
    connector_client.connect(reconnect=True)


    device = Device(device_id, device_name, device_type_id)
    service = "SENSOR"

    try:
        connector_client.add_device(device)
    except Exception as e:
        pass 

    for sample in load_data(file_path):
        print("send sample")
        message = DeviceMessage(data=sample)
        envelope = EventEnvelope(device=device, service=service, message=message)
        connector_client.send_event(envelope)
