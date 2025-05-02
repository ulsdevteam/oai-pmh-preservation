import os
import httpx
import requests
from datetime import datetime, date
from oaipmh_scythe import Scythe
import certifi
#import truststore
#truststore.inject_into_ssl()

# Configure SSL verification
USE_SSL_VERIFICATION = True  # Set to False to bypass SSL verification (not recommended)

# Ensure the system uses certifi's certificates
if USE_SSL_VERIFICATION:
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["SSL_CERT_DIR"] = os.path.dirname(certifi.where())


def main():
    config = readConfigFile()
    base_url = config["base_url"]
    metadata_format = config["metadata_format"]
    last_run_date, today = readStateFile()

    # Resolve relative path to absolute path
    storage_dir = os.path.abspath(config["storage_directory"])
    config["storage_directory"] = storage_dir  # Update config with resolved path

    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)


    try:
        runScythe(base_url, metadata_format, last_run_date, today, config)
    except Exception as e:
        print(f"Error running Scythe: {e}")
    updateStateFile(today)


def readConfigFile():
    config = {}
    with open("config.txt", "r") as configFile:
        for line in configFile:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
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


def extract_file_uris(metadata_dict, xpath_expr=None):
    file_uris = []

    # Search for values in the dict that look like URLs
    for key, value in metadata_dict.items():
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.startswith("http"):
                    file_uris.append(item)
        elif isinstance(value, str) and value.startswith("http"):
            file_uris.append(value)

    return file_uris


def process_records(records, config):
    for record in records:
        header = record.header
        identifier = header.identifier

        storage_path = os.path.join(config['storage_directory'], identifier.replace(":", "_"))
        if os.path.exists(storage_path):
            try:
                os.rmdir(storage_path)
            except OSError:
                pass  # Skip if directory isn't empty or removable

        os.makedirs(storage_path, exist_ok=True)

        metadata = record.metadata  # dict
        if metadata:
            metadata_file_path = os.path.join(storage_path, f"{identifier.replace(':', '_')}.{config['metadata_format']}")

            with open(metadata_file_path, 'w', encoding='utf-8') as file:
                file.write(str(metadata))  # Save as string for now

            # Extract and download files
            file_uris = extract_file_uris(metadata, config.get('xpath'))
            for file_uri in file_uris:
                fetch_and_store_file(file_uri, storage_path, identifier)


def fetch_and_store_file(file_uri, storage_path, identifier):
    try:
        file_uri = file_uri.replace("https:", "http:")
        response = requests.get(file_uri, verify=certifi.where())  # <-- Use certifi directly here
        response.raise_for_status()
        file_name = os.path.basename(file_uri)
        file_path = os.path.join(storage_path, 'files', file_name)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {file_uri}")
    except Exception as e:
        print(f"Failed to download {file_uri}: {e}")


def runScythe(endpoint, metadata_format, last_run_date, today, config):
    print(f"Querying endpoint: {endpoint} with format: {metadata_format} from {last_run_date} to {today}")
    try:
        with Scythe(endpoint) as scythe:
            records = scythe.list_records(
                metadata_prefix=metadata_format,
                from_=last_run_date.strftime("%Y-%m-%d"),
                until=today.strftime("%Y-%m-%d")
            )
            tempNumRecords = 0
            process_records(records, config)
            for index, record in enumerate(records):
                print(record)
                tempNumRecords += 1
                if tempNumRecords == 100:
                    break  # Stop after 100 records for testing
    except Exception as e:
        print(f"No records found or error occurred: {e}")


if __name__ == "__main__":
    main()
