
# local imports
from auth import ConfigData, login

# type imports
from pathlib import Path
from typing import Iterator
from oaipmh_scythe.models import OAIItem

# inbuilt imports
import os
from urllib.parse import urlparse
import logging
from itertools import islice
from datetime import datetime, date
import logging
import sys
from collections import defaultdict
import shutil

# library imports
from lxml import etree
from oaipmh_scythe import Scythe
import httpx

#tomllib is inbuilt >=3.11. Use tomlli as fallback
try:
    import tomllib
except ImportError:
    try:
        import tomlli as tomllib
    except:
        log.err("toml library not found. "
            "")
        raise SystemExit(1)

#setup logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log = logger
identifiers_being_updated = set()
request_client = None

def is_valid_url(url:str):
    """
    checks if url provided is valid url 
    code snippet from https://stackoverflow.com/a/38020041
    """
    try:
        res = urlparse(url)
        return all([res.scheme, res.netloc])
    except Exception as e:
        log.debug(f"{url} is not a url")
        return False

def preprocess_config(config:dict):
    """
    preprocess options provided in toml. Following operations
    are done on each of the options

    - storage_directory: path provided converted to absolute path
    """
    storage_dir = config.get("storage_directory")
    abspath = os.path.abspath

    if storage_dir is not None:
        config["storage_directory"] = abspath(storage_dir)
            

def load_config(conf_location : str | Path = "config.txt") -> dict:
    """
    read toml conf_location and output dict containing config
    options.

    Note: namespaces is a table in config toml and gets translated
        as a dictionary which is desired behavior
    """
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
     
def load_last_run(state_location : Path | str = "state.txt") -> (datetime.date, datetime.date):
    """
    load date stored on filesystem on which date script
    was last executed, and the current date today
    """
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

def update_last_run(state_location : Path | str = "state.txt"):
    """
    update date stored on filesystem on which date script
    was last executed with the current date today
    """
    logging.warn("Not updating for debug")
    pass


def authenticate_scythe(client: Scythe, config:dict) -> Scythe:
    """
    Add auth information to underlying httpx client in 
    scythe client. Auth types supported

    basic -> basic http username:password
    cookie -> a login form is submitted to a user provided website,
        and response is set as header
    """ 
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
    client.client.headers.update(login_headers)
    return client

def fetch_metadata_records(scythe_client:Scythe, metadata_format:str,
    config:dict) -> Iterator[OAIItem]:
    """
    perform ListRecords OAI verb from last run to today, 
    and grab first config["limit_entries"] results 
    """
    records = scythe_client.list_records(
        metadata_prefix = metadata_format,
        from_ = config["last_run"], until = config["today"]) 
    
    records = islice(records, config["limit_entries"])
    return records
  

def get_record_header_info(basepath:Path | str, record: OAIItem) -> (Path | str, str):
    """
    extract identifier from record header and the corresponding
    path where record would be stored in filesystem
    """
    pathjoin = os.path.join

    identifier = record.header.identifier  
    identifier = identifier.replace(":", "_")
    record_path = os.path.join(basepath, identifier)
    return record_path, identifier

def save_metadata_record(record:OAIItem, metadata_format:str, config: dict):
    """
    Save <metadata_format> metadata file which is associated 
    with record, onto disk. Out of date files are removed, 
    if they existed
    """
    pathjoin = os.path.join
    identifier, record_path = get_record_header_info(
            config["storage_directory"], record
        )
    
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
    

def extract_file_uris(record_xml: str | bytes, xpath_expr:str, ns_dict:str) -> list[str]:
    """
    record_xml: metadata xml file present in OAI-PMH
    xpath_expr: user provided xpath that extracts URIs where
        files to be preserved are avavilable
    ns_dict: namespace aliases used in xpath_expr

    returns: list of results of xpath_expr applied on record_xml which are valid
        uris
    """
    tree = etree.XML(str(record_xml))
    logger.debug(ns_dict)
    vals = tree.xpath(xpath_expr, namespaces=ns_dict)
    logger.debug(vals, [is_valid_url(x) for x in vals])
    return list(filter(is_valid_url, tree.xpath(xpath_expr, namespaces=ns_dict)))  

def get_default_http_client():
    # return an authenticated httpx client
    return httpx # placeholder: replace with authenticated client
        
def save_metadata_file(record:OAIItem, metadata_format:str, config:dict):
    """
    Extract URIs from metadata file in record, fetch files,
    and save results on disk
    """
    record_path, _ = get_record_header_info(config["storage_directory"], record)
    files_folder = os.path.join(record_path, "files")
    if not os.path.exists(files_folder):
        os.makedirs(files_folder, exist_ok = True)
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
            meta_format_str = meta_format.metadataPrefix
            records = fetch_metadata_records(
                            scythe, 
                            meta_format_str,
                            config)
            for record in records:
                save_metadata_record(record, meta_format_str, 
                    config)
                if meta_format_str == config["metadata_format"]:
                    logger.info("saving metadata files")
                    save_metadata_file(record, meta_format_str, config)
                


if __name__ == '__main__':
    main()
