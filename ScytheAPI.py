from oaipmh_scythe import Scythe
from datetime import datetime, date
import httpx
import certifi
import os

# Configure SSL verification
USE_SSL_VERIFICATION = True  # Set to False if you want to bypass SSL verification (not recommended)

# Ensure the system uses certifi's certificates
if USE_SSL_VERIFICATION:
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["SSL_CERT_DIR"] = os.path.dirname(certifi.where())

def main():
    config = readConfigFile()
    base_url = config["base_url"]
    metadataFormat = config["metadata_format"]

    last_run_date, today = readStateFile()
    
    runScythe(base_url, metadataFormat, last_run_date, today)

    # After successful processing, update the state file with today's date
    updateStateFile(today)

def readConfigFile():
    config = {}
    with open("config.txt", "r") as configFile:
        for line in configFile:
            line = line.strip()
            if not line or "=" not in line:  # Skip empty or invalid lines
                continue
            key, value = line.split("=", 1)
            config[key] = value
    return config

def readStateFile():
    try:
        with open("state.txt", "r") as stateFile:
            last_run_str = stateFile.read().strip()
            last_run_date = datetime.strptime(last_run_str, "%Y-%m-%d").date()
    except (FileNotFoundError, ValueError):
        print("State file not found or invalid format. Defaulting to yesterday.")
        last_run_date = date.today()
    
    today = date.today()
    return last_run_date, today

def updateStateFile(new_date):
    with open("state.txt", "w") as stateFile:
        stateFile.write(new_date.strftime("%Y-%m-%d"))
    print(f"Updated state file with {new_date}")

def runScythe(endpoint, metadataFormat, last_run_date, today):
    # Use Scythe without modifying `client` directly
    with Scythe(endpoint) as scythe:
        records = scythe.list_records(
            metadata_prefix=metadataFormat, 
            from_=last_run_date.strftime("%Y-%m-%d"), 
            until=today.strftime("%Y-%m-%d")
        )

        tempNumRecords = 0
        for record in records:
            print(record)  # Process the record as needed
            tempNumRecords += 1
            if tempNumRecords == 100:
                break  # Stop after 100 records for testing

if __name__ == "__main__":
    main()
