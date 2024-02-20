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
git clone https://your-repository-url.git
cd AIS-Data-Processor
pip install -r requirements.txt
python ais.py
```
