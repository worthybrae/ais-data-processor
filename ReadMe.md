# AIS Data Processor

This project is designed to handle and process Automatic Identification System (AIS) messages at high throughput, specifically targeting 300 messages per second. It features the capability to add a unique MD5 hash to each message based on specific attributes and save the processed data into compressed CSV files for efficient storage.

## Features

- **High Throughput Processing**: Efficiently processes up to 300 AIS messages per second.
- **MD5 Hash Generation**: Adds a unique MD5 hash for each message based on MMSI, truncated latitude and longitude, and timestamp.
- **Data Compression**: Saves processed messages in compressed CSV files (gzip format) to minimize storage requirements.
- **Dynamic Path Creation**: Automatically creates directory paths based on message timestamps for organized data storage.

## Installation

To set up this project, clone the repository to your local machine and install the required Python packages.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Credentials

Get your aisstream.io key [here](https://aisstream.io/) after making an account. Once you have your key:

1. Duplicate the .env.example file
2. Rename the duplicated file to .env
3. Replace 'xxxxxx' with your aisstream key

## Requirements

You need the following requirements in order for this to run smoothly:

1. Python 3.11+
2. 2GB of avilable storage for a single day of data collection
3. 54GB of available storage for a month of data collection
4. 657GB of available storage for a year of data collection

## Running

To start collecting data just run the following command in your recently made virtual enviornment:

```bash
python ais.py
```
