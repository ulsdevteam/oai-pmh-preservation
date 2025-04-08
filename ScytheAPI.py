import os
import httpx
import requests
from datetime import datetime, date
from oaipmh_scythe import Scythe
import certifi
from lxml import etree as ET  # For proper namespace and XPath handling

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

    # Create storage directory if it doesn't exist
    if not os.path.exists(config["storage_directory"]):
        os.makedirs(config["storage_directory"])

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


def extract_file_uris(metadata, xpath_expr):
    # Parse the XML content from the metadata string
    root = ET.fromstring(metadata.encode("utf-8"))

    # Define namespaces used in the XML
    namespaces = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'dc': 'http://purl.org/dc/elements/1.1/'
    }

    # Evaluate XPath and filter for HTTP links
    file_uri_elements = root.xpath(xpath_expr, namespaces=namespaces)
    file_uris = [element.strip() for element in file_uri_elements if element.strip().startswith('http')]

    return file_uris


def process_records(records, config):
    for record in records:
        identifier = record.identifier
        storage_path = os.path.join(config['storage_directory'], identifier)

        if os.path.exists(storage_path):
            try:
                os.rmdir(storage_path)
            except OSError:
                pass  # Skip if directory isn't empty or removable

        os.makedirs(storage_path, exist_ok=True)

        metadata_formats = record.get_metadata_formats()
        for metadata_format in metadata_formats:
            metadata = record.get_metadata(metadata_format)
            metadata_file_path = os.path.join(storage_path, f"{identifier}.{metadata_format}")

            with open(metadata_file_path, 'w', encoding='utf-8') as file:
                file.write(metadata)

            if metadata_format == config['metadata_format']:
                file_uris = extract_file_uris(metadata, config['xpath'])
                for file_uri in file_uris:
                    fetch_and_store_file(file_uri, storage_path, identifier)


def fetch_and_store_file(file_uri, storage_path, identifier):
    try:
        response = requests.get(file_uri)
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
