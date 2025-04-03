import os
import httpx
from datetime import datetime, date
from oaipmh_scythe import Scythe
import certifi
from fpdf import FPDF

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

    # Create metadata folder if it doesn't exist
    if not os.path.exists("metadata"):
        os.makedirs("metadata")

    try:
        runScythe(base_url, metadataFormat, last_run_date, today, config)
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

def process_records(records, config):
    for record in records:
        identifier = record.identifier
        storage_path = os.path.join(config['metadata'], identifier)
        
        if os.path.exists(storage_path):
            os.rmdir(storage_path)
        
        os.makedirs(storage_path, exist_ok=True)
        
        metadata_formats = record.get_metadata_formats()
        for metadata_format in metadata_formats:
            metadata = record.get_metadata(metadata_format)
            metadata_file_path = os.path.join(storage_path, f"{identifier}.{metadata_format}")
            
            with open(metadata_file_path, 'w') as file:
                file.write(metadata)
            
            if metadata_format == config['FILES_METADATA']: #need to check this 
                file_uris = extract_file_uris(metadata, config['FILES_XPATH']) #need to add this as well
                for file_uri in file_uris:
                    fetch_and_store_file(file_uri, storage_path, identifier)


def extract_file_uris(metadata, xpath):
    # Parse the XML content from the metadata
    root = ET.fromstring(metadata)
    #need to investigate this and import the library
    
    # Use XPath to find all matching elements
    file_uri_elements = root.findall(xpath)
    
    # Extract the text content of each matching element
    file_uris = [element.text for element in file_uri_elements]
    
    return file_uris

def fetch_and_store_file(file_uri, storage_path, identifier):
    response = requests.get(file_uri)
    file_name = os.path.basename(file_uri)
    file_path = os.path.join(storage_path, 'files', file_name)
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'wb') as file:
        file.write(response.content)


def runScythe(endpoint, metadataFormat, last_run_date, today, config):
    print(f"Querying endpoint: {endpoint} with format: {metadataFormat} from {last_run_date} to {today}")
    try:
        with Scythe(endpoint) as scythe:
            records = scythe.list_records(
                metadata_prefix=metadataFormat,
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
