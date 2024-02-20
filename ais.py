from dotenv import load_dotenv
import asyncio
import websockets
import json
import os
import json
from datetime import datetime
import hashlib
import pandas as pd


class AISData:
    def __init__(self, ais_message_bytes):
        # Decode byte string to string
        message_str = ais_message_bytes.decode('utf-8')
        
        # Parse the JSON string into a dictionary
        message_dict = json.loads(message_str)

        # Access the metadata component of the JSON
        message_data = message_dict['MetaData']

        # Define object attributes
        self.mmsi = message_data['MMSI']
        self.ship_name = message_data['ShipName']
        self.latitude = float(message_data['latitude'])
        self.longitude = float(message_data['longitude'])
        timestamp_list = message_data['time_utc'].split(' ')
        formatted_time = timestamp_list[1][:-3]
        self.timestamp = datetime.strptime(' '.join([timestamp_list[0], formatted_time, timestamp_list[2], timestamp_list[3]]), '%Y-%m-%d %H:%M:%S.%f %z UTC')
        ifl_hash_str = f"{self.mmsi}{self.latitude:.4f}{self.longitude:.4f}{self.timestamp.strftime('%Y-%m-%d %H:%M')}"
        self.ifl_hash = hashlib.md5(ifl_hash_str.encode()).hexdigest()
        dup_hash_str = f"{self.mmsi}{self.latitude}{self.longitude}{self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}"
        self.dup_hash = hashlib.md5(dup_hash_str.encode()).hexdigest()
        self.ingested_at = datetime.utcnow()

    def __str__(self):
        return f"Ship Name: {self.ship_name}\nLatitude: {self.latitude:.6f}\nLongitude: {self.longitude:.6f}\nTimestamp: {datetime.strftime(self.timestamp, '%Y-%m-%d %H:%M %z')}"

class AISDataStream:
    def __init__(self):
        self.messages = []
        self.last_hour = None
        self.last_timestamp = None

    def add_new_message(self, message):
        self.messages.append([
            message.mmsi, 
            message.ship_name,
            message.latitude,
            message.longitude,
            message.timestamp,
            message.ifl_hash,
            message.dup_hash,
            message.ingested_at
        ])
        self.check_if_new_hour(message.timestamp.hour, f'{message.timestamp.year}-{message.timestamp.month}-{message.timestamp.day}')

    def check_if_new_hour(self, hour, date):
        if self.last_hour == None:
            self.last_hour = int(hour)
            self.last_date = date
        elif int(hour) != self.last_hour:
            self.save_messages()
            self.last_hour = int(hour)
            self.last_date = date
        else:
            pass

    def save_messages(self):
        df = pd.DataFrame(self.messages, columns=[
            'mmsi', 
            'ship_name', 
            'latitude', 
            'longitude', 
            'timestamp',
            'ifl_hash',
            'dup_hash',
            'ingested_at'
        ])
        year = self.last_date.split('-')[0]
        month = self.last_date.split('-')[1]
        day = self.last_date.split('-')[2]
        hour = self.last_hour

        # Create directory path
        dir_path = os.path.join('data', year, month, day)
        
        # Check if the directory path exists, if not create it
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)  # This will create all intermediate directories as well
        
        # Save DataFrame to a compressed CSV in append mode
        file_path = os.path.join(dir_path, f"{hour}.csv.gz")

        # Write to a compressed CSV file
        df.to_csv(file_path, mode='a', index=False, compression='gzip')

        # Clear the messages list
        self.messages = []


load_dotenv(override=True)

async def ais_stream():
    url = "wss://stream.aisstream.io/v0/stream"
    
    # Construct the subscription message according to the documentation.
    subscription_message = {
        "APIKey": os.getenv("AIS_STREAM_KEY"),  # Replace with your actual API key
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport"] 
    }
    
    # Convert the message to a JSON formatted string
    subscription_message_json = json.dumps(subscription_message)

    data_stream = AISDataStream()

    async with websockets.connect(url) as websocket:
        # Send the subscription message within 3 seconds of opening the connection
        await websocket.send(subscription_message_json)
        
        # Wait for a response to confirm subscription
        response = await websocket.recv()
        print(f"Subscription confirmation: {response}")

        # Now listen for incoming AIS data messages
        async for message in websocket:
            ais_obj = AISData(message)
            data_stream.add_new_message(ais_obj)
            print(ais_obj)

# Run the coroutine
asyncio.run(ais_stream())
