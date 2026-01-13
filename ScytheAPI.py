import os
import httpx
import requests
from datetime import datetime, date
from oaipmh_scythe import Scythe
import ssl
import certifi
import lxml
from lxml import etree
from lxml.builder import ElementMaker
import shutil
import truststore
import pathlib
import hashlib
import auth     # local import 

from urllib.parse import urlparse
truststore.inject_into_ssl()

def is_valid_url(url):
    try:
        res = urlparse(url)
        return all([res.scheme, res.netloc])
    except:
        return False
# Configure SSL verification
USE_SSL_VERIFICATION = True  # Set to False to bypass SSL verification (not recommended)

# Ensure the system uses certifi's certificates
"""
if USE_SSL_VERIFICATION:
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["SSL_CERT_DIR"] = os.path.dirname(certifi.where())

"""

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
    print("Not Updating state file for debug!")
    return
    with open("state.txt", "w") as stateFile:
        stateFile.write(new_date.strftime("%Y-%m-%d"))
    print(f"Updated state file with {new_date}")

def extract_file_uris(record_xml, xpath_expr, ns_dict):
    tree = etree.XML(str(record_xml))
    return list(filter(is_valid_url, tree.xpath(xpath_expr, namespaces=ns_dict)))  

#def extract_file_uris(metadata_dict, xpath_expr=None):
#    file_uris = []
#    print(metadata_dict.items())

    # Search for values in the dict that look like URLs
#    for key, value in metadata_dict.items():
#        if isinstance(value, list):
#            for item in value:
#                if isinstance(item, str) and item.startswith("http"):
#                    file_uris.append(item)
#        elif isinstance(value, str) and value.startswith("http"):
#            file_uris.append(value)
#
#    return file_uris
def setup_dir_for_update(storage_path):
    """
    A record is stored in a folder corresponding to it's id.
    When a record is being processed, the corresponding folder may be in one 
    of these states:

    1. folder does not exist
    2. folder exists, but contains out of date information
    3. folder exists, but is in a partially updated state 
    
    Distinguishibg between case 2 and case 3 is necessary as updates to the 
    folder are done out of order (by metadata format instead of per record).
    
    Case 2 and Case 3 are distinguished via presence of a trunc file called 
    .updated
    """
    placeholder_path = os.path.join(storage_path, ".update")
    if not os.path.exists(storage_path):
        os.makedirs(storage_path)
        open(placeholder_path, 'w').close() # create a trunc file
        return
    if not os.path.exists(placeholder_path):
        shutil.rmtree(storage_path)
        os.makedirs(storage_path)
        open(placeholder_path, 'w').close()
    
def dir_postaction(main_dir):
    for dir in os.listdir(main_dir):
        placeholder_path = os.path.join(main_dir, dir, ".update")
        if os.path.exists(placeholder_path):
            create_folder_opex(os.path.join(main_dir, dir))
            os.remove(placeholder_path)

def create_folder_opex(dir_path):
     
    pass 

def process_records(records, config, format, save_files = False):
    for record, _ in zip(records, range(10)):
        header = record.header
        identifier = header.identifier

        storage_path = os.path.join(config['storage_directory'], identifier.replace(":", "_"))
        setup_dir_for_update(storage_path)
        """
        if os.path.exists(storage_path):
            try:
                os.rmdir(storage_path)
                py_path = os.path.realpath(__file__)
                working_dir = pathlib.Path(py_path).parent
                if (working_dir not in
                    pathlib.Path(os.path.realpath(storage_path)).parents):
                    print("Error: storage_directory ")
                    raise Exception("Error:")
                #shutil.rmtree(storage_path)
            except OSError:
                pass  # Skip if directory isn't empty or removable

        os.makedirs(storage_path, exist_ok=True)
        """
        metadata = record.metadata  # dict
        #print(etree.tostring(etree.XML(str(record)),
        #    pretty_print=True).decode("utf-8"))
        if metadata:
            metadata_file_path = os.path.join(storage_path,
                f"{identifier.replace(':', '_')}.{format}")

            with open(metadata_file_path, 'w', encoding='utf-8') as file:
                file.write(str(record))  # Save as string for now
            
            if save_files:
                # Extract and download files
                file_uris = extract_file_uris(record, "//dc:identifier/text()", 
                    {'dc': "http://purl.org/dc/elements/1.1/"})
                print(file_uris)
                for i, file_uri in enumerate(file_uris):
                    #input("Getting "+ file_uri)
                    file_data = fetch_file(file_uri)
                    filename = os.path.basename(file_uri) 
                    generated_opex = generate_opex_file(file_data, str(record),
                        filename, f"{identifier}_{i}") 
                    
                    store_full_path = os.path.join(storage_path, "files",
                        filename)
                    store_file(file_data, store_full_path)
                    store_file(etree.tostring(generated_opex, encoding="UTF-8", 
                        standalone=True, pretty_print=True), store_full_path + ".opex")
                    #fetch_and_store_file(file_uri, storage_path, identifier)


def fetch_file(file_uri):
    try:
        response = requests.get(file_uri)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Failed to download {file_uri}: {e}")


opex_generated_count = 0 # used for sourceID temporarily
def generate_opex_file(file_data, metadata, filename, identifier):
    global opex_generated_count
    # currently specialized for oai_dc 
    opex_ns_url = "http://www.openpreservationexchange.org/opex/v1.2"
    oai_ns_url = "http://www.openarchives.org/OAI/2.0/"
    dc_ns_url = "http://purl.org/dc/elements/1.1/"
    
    nsmap = {'opex': opex_ns_url, 'oai':oai_ns_url, 'dc':dc_ns_url}
    metadata_etree = etree.XML(metadata)
    title = metadata_etree.xpath("//dc:title/text()", namespaces=nsmap)[0]  
    description = metadata_etree.xpath("//dc:description/text()",
            namespaces=nsmap)[0]  
    identifiers = metadata_etree.xpath("//dc:identifiers/text()",
        namespaces=nsmap)
    
    
    E = ElementMaker(namespace=opex_ns_url, 
        nsmap={'opex': opex_ns_url})
    
    identifier_opex_element = E.Identifiers(*[E.Identifier(x) for x in
        identifiers])
    fixities = E.Fixities(
        E.Fixity(hashlib.sha256(file_data).hexdigest(), {'type': 'SHA-256'}),
        )
    
    
    root = E.OPEXMetadata(
        E.Properties(
            E.Title(title), E.Decription(description), identifier_opex_element 
        ),
        E.Transfer(
            E.SourceID(identifier),
            fixities,
            filename
        )
    )    
    return root
    

def store_file(file_data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'wb') as f:
        f.write(file_data)

def fetch_and_store_file(file_uri, storage_path, identifier):
    try:
        #file_uri = file_uri.replace("https:", "http:")
        response = requests.get(file_uri)  # <-- Use certifi directly here
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
    auth = httpx.BasicAuth(username="pals", password="pals")
    try:
        with Scythe(endpoint, auth=auth) as scythe:
            #print(
            #    scythe.client.get(
            #        "https://pittir.hykucommons.org/catalog/oai?verb=Identify"
            #    ).headers)
            #print(scythe.identify())
            
            metadata_formats = scythe.list_metadata_formats()
            for meta_format in metadata_formats:
                print(f"{meta_format.metadataPrefix}")
                format_prefix = meta_format.metadataPrefix
                records = scythe.list_records(
                    metadata_prefix=format_prefix,
                    from_=last_run_date.strftime("%Y-%m-%d"),
                    until=today.strftime("%Y-%m-%d")
                )
                print("Got records!")
                process_records(records, config, format_prefix, format_prefix == metadata_format)
        dir_postaction(config["storage_directory"])
    except Exception as e:
        print(f"No records found or error occurred: {e}")
        


if __name__ == "__main__":
    main()
