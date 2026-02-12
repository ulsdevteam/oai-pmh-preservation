from auth import ConfigData, login
import os
from urllib.parse import urlparse
import logging
from itertools import islice
from datetime import datetime, date
from oaipmh_scythe import Scythe
import httpx
import logging
import sys
from collections import defaultdict
import shutil
from lxml import etree
logging.basicConfig(level = logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

identifiers_being_updated = set()
request_client = None

try:
    import tomllib
except ImportError:
    try:
        import tomlli as tomllib
    except:
        log.err("toml library not found")
        raise SystemExit(1)
logger = logging

def is_valid_url(url):
    try:
        res = urlparse(url)
        return all([res.scheme, res.netloc])
    except Exception as e:
        print(e)
        return False

def preprocess_config(config):
    storage_dir = config.get("storage_directory")
    abspath = os.path.abspath

    if storage_dir is not None:
        config["storage_directory"] = abspath(storage_dir)
            

def load_config(conf_location = "config.txt"):
    defaults = {
        "limit_entries": 5
    }
    last_run, today = load_last_run()
    with open(conf_location, "rb") as conf_file:
        conf = tomllib.load(conf_file)
        used_defaults = {(k,v) for k, v in defaults.items()
            if k not in conf}
        conf["last_run"], conf["today"] = last_run, today
        logging.info(f"conf provided: {conf}")
        logging.info(f"Using defaults: {used_defaults}")
        return defaults | conf
     
def load_last_run(state_location = "state.txt"):
    try:
        with open(state_location, "r") as stateFile:
            last_run_str = stateFile.read().strip()
            last_run_date = datetime.strptime(last_run_str, 
                    "%Y-%m-%d").date()
    except (FileNotFoundError, ValueError):
        print("State file not found or invalid format. " 
            "Defaulting to yesterday.")
        last_run_date = date.today()
    today = date.today()
    return last_run_date, today

def update_last_run(state_location = "state.txt"):
    logging.warn("Not updating for debug")
    pass


def authenticate_scythe(client, config):
    if config["login_type"] == "basic":
        client.client.auth = httpx.BasicAuth(
                                config["username"], 
                                config["password"])
        return client
    auth_conf = ConfigData(
        login_uri = config["login_uri"],
        login_uname_el = config["login_username_xpath"],
        login_passwd_el = config["login_password_xpath"],
        login_form_el = config["login_form_xpath"],
        gather_header = "Set-Cookie",
        send_header = "Cookie"
    )
    login_headers = login(auth_conf, config["username"],
                                config["password"])
    
    return client

def fetch_metadata_records(scythe_client, metadata_format, 
    config):
    records = scythe_client.list_records(
        metadata_prefix = metadata_format.metadataPrefix,
        from_ = config["last_run"], until = config["today"]) 
    
    records = islice(records, config["limit_entries"])
    return records
  

def get_record_header_info(basepath, record):
    pathjoin = os.path.join

    identifier = record.header.identifier  
    identifier = identifier.replace(":", "_")
    record_path = os.path.join(basepath, identifier)
    return record_path, identifier
def save_metadata_record(record, metadata_format, config):
    pathjoin = os.path.join
    metadata_format = metadata_format.metadataPrefix
    
    identifier = record.header.identifier  
    identifier = identifier.replace(":", "_")
    record_path = pathjoin(config["storage_directory"],
                        identifier)
    
    metadata_file_path = pathjoin(record_path, 
            f"{identifier}.{metadata_format}")  
    if not os.path.exists(record_path):
        os.makedirs(record_path)
    elif identifier not in identifiers_being_updated:
        shutil.rmtree(record_path)
        os.makedirs(record_path)
        identifiers_being_updated.add(identifier)
    with open(metadata_file_path, 'w', 
            encoding='utf-8') as f:
        log.info(f"writing file: {metadata_file_path}")
        f.write(str(record))  # Save as string for now
    

def extract_file_uris(record_xml, xpath_expr, ns_dict):
    tree = etree.XML(str(record_xml))
    print(ns_dict)
    vals = tree.xpath(xpath_expr, namespaces=ns_dict)
    print(vals, [is_valid_url(x) for x in vals])
    return list(filter(is_valid_url, tree.xpath(xpath_expr, namespaces=ns_dict)))  
    pass

def get_default_http_client():
    # return an authenticated httpx client
    return httpx # placeholder: replace with authenticated client
        
def save_metadata_file(record, metadata_format, config):
    record_path, _ = get_record_header_info(config["storage_directory"], record)
    metadata_format = metadata_format.metadataPrefix
    file_uris = extract_file_uris(record, config["xpath"], config["namespaces"])
    print(file_uris)
    for uri in file_uris:
        logger.info(f"getting uri {uri}")
        client = get_default_http_client()
        try:
            response = client.get(uri)
            response.raise_for_status()
        except Exception as e:
            logger.warn(f"failed to get {uri} due to {e}")
            continue
        file_name = os.path.basename(uri)
        file_path = os.path.join(record_path, "files", file_name)
        with open(file_path, "w") as f:
            logger.info(f"saving {uri} @ {file_path}")
            f.write(response.text)
        
def main():
    config = load_config()
    preprocess_config(config)
    with Scythe(config["base_url"]) as scythe:
        authenticate_scythe(scythe, config)
        formats = scythe.list_metadata_formats()
        for meta_format in formats:
            records = fetch_metadata_records(
                            scythe, 
                            meta_format,
                            config)
            for record in records:
                save_metadata_record(record, meta_format, 
                    config)
                if meta_format.metadataPrefix == config["metadata_format"]:
                    logger.info("saving metadata files")
                    save_metadata_file(record, meta_format, config)
                


if __name__ == '__main__':
    main()
