import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from oaipmh_scythe import Scythe

def read_configuration(config_file):
    with open(config_file, 'r') as file:
        config = {}
        for line in file:
            key, value = line.strip().split('=')
            config[key] = value
        return config

def read_state(state_file):
    try:
        with open(state_file, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def update_state(state_file, until_date):
    with open(state_file, 'w') as file:
        file.write(until_date)

def fetch_records(config, from_date, until_date):
    client = Scythe(config['ENDPOINT'])
    records = client.get_records(from_date, until_date)
    return records

def process_records(records, config):
    for record in records:
        identifier = record.identifier
        storage_path = os.path.join(config['STORAGE'], identifier)
        
        if os.path.exists(storage_path):
            os.rmdir(storage_path)
        
        os.makedirs(storage_path, exist_ok=True)
        
        metadata_formats = record.get_metadata_formats()
        for metadata_format in metadata_formats:
            metadata = record.get_metadata(metadata_format)
            metadata_file_path = os.path.join(storage_path, f"{identifier}.{metadata_format}")
            
            with open(metadata_file_path, 'w') as file:
                file.write(metadata)
            
            if metadata_format == config['FILES_METADATA']:
                file_uris = extract_file_uris(metadata, config['FILES_XPATH'])
                for file_uri in file_uris:
                    fetch_and_store_file(file_uri, storage_path, identifier)


def extract_file_uris(metadata, xpath):
    # Parse the XML content from the metadata
    root = ET.fromstring(metadata)
    
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

def main():
    config_file = 'config.txt'
    state_file = 'state.txt'
    
    config = read_configuration(config_file)
    from_date = read_state(state_file)
    until_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    records = fetch_records(config, from_date, until_date)
    process_records(records, config)
    
    update_state(state_file, until_date)

if __name__ == "__main__":
    main()
