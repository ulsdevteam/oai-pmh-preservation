
# local imports
from auth import ConfigData, login

# type imports
from pathlib import Path
from typing import Iterator
from oaipmh_scythe.models import OAIItem

# inbuilt imports
import time
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
import pyrfc6266
from upath import UPath

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
    
    if storage_dir is not None:
        abspath = os.path.abspath(config["storage_directory"])
        if abspath != config["storage_directory"]:
            log.warn(f"path {storage_dir} "
                f"is ambiguous. Using path {abspath}")
        config["storage_directory"] = abspath
            

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
    last_run = load_last_run(state_location = "state.txt")
    today = datetime.now()
    
    with open(conf_location, "rb") as conf_file:
        conf = tomllib.load(conf_file)
        used_defaults = {(k,v) for k, v in defaults.items()
            if k not in conf}
        #last_run, today = None, None
        #if conf.get("fetch_all") == 1:
        if conf.get("mode") is None or conf["mode"] == "harvest": 
            conf["_from"] = last_run
            conf["_until"] = conf.get("until", today)
        elif conf.get("mode") == "fetch":
            conf["_from"] = conf.get("from", None)
            conf["_until"] = conf.get("until", None) 
        
        if isinstance(conf["_from"], str):
            conf["_from"] = datetime.fromisoformat(conf["_from"])
        
        if isinstance(conf["_until"], str):
            conf["_until"] = datetime.fromisoformat(conf["_until"])
        #print(last_run)
        #raise SystemExit(1)
        logging.info(f"conf provided: {conf}")
        logging.info(f"Using defaults: {used_defaults}")
        return defaults | conf

def load_last_run(state_location : Path | str = "state.txt") -> str | datetime | None:
    """
    load date stored on filesystem on which date script
    was last executed, and the current date today
    """
    try:
        with open(state_location, "r") as stateFile:
            last_run_str = stateFile.read().strip()
            last_run_date = datetime.fromisoformat(last_run_str)
            #last_run_date = datetime.strptime(last_run_str, 
            #        "%Y-%m-%d").date()
            return last_run_date
            
    except (FileNotFoundError, ValueError):
        log.warn("State file not found or invalid format. " 
            "Defaulting to fetching from earliest record.")
        return None
    

def update_last_run(state_location : Path | str = "state.txt", value: datetime = None):
    """
    update date stored on filesystem on which date script
    was last executed with the current date today
    """
    if value is None or not isinstance(value, datetime):
        logger.warning("Invalid Update date provided, skipping update...")
    with open(state_location, 'w') as f:
        f.write(value.isoformat())
    #logging.warn("Not updating for debug")
    pass


def authenticate_scythe(client: Scythe, config:dict) -> Scythe:
    """
    Add auth information to underlying httpx client in 
    scythe client. Multiple auth types can be provided in 
    config["login_types"] at the same time as a list
    Auth types supported

    basic -> basic http username:password
    cookie -> a login form is submitted to a user provided website,
        and response is set as header
    """ 
    basic_auth_var = None
    if "basic" in config["login_types"]:
        basic_auth_var = httpx.BasicAuth(
                                    config["basic"]["username"], 
                                    config["basic"]["password"])
        client.client.auth = basic_auth_var
    for login_type in config["login_types"]:
        if login_type == "basic":
            continue
            
        if login_type == "cookie":
            auth_conf = ConfigData(
                login_uri = config["login_uri"],
                login_uname_el = config["cookie"]["username_xpath"],
                login_passwd_el = config["cookie"]["password_xpath"],
                login_form_el = config["cookie"]["form_xpath"],
                gather_header = "Set-Cookie",
                send_header = "Cookie"
            )
            login_headers = login(auth_conf, config["cookie"]["username"],
                                        config["cookie"]["password"], basic_auth = basic_auth_var)
            print(login_headers)
            client.client.headers.update(login_headers)
    global request_client
    if request_client is None:
        request_client = client.client # use the same client
    return client

def fetch_metadata_records(scythe_client:Scythe, metadata_format:str,
    config:dict, set_ = None) -> Iterator[OAIItem]:
    """
    perform ListRecords OAI verb from last run to today, 
    and grab first config["limit_entries"] results 
    """
    logger.info(f"{config['_from']}")
    print(set_)
    records = scythe_client.list_records(
        metadata_prefix = metadata_format,
        from_ = config["_from"], until = config["_until"], set_ = set_) 
    print(next(records))
    # consume iterator skip_count times
    #[None for _ in islice(records, config["skip_count"]) if False] 
    records = islice(records, config["limit_entries"])
    return records
  

def get_record_header_info(basepath:Path | str, record: OAIItem) -> (Path | str, str):
    """
    extract identifier from record header and the corresponding
    path where record would be stored in filesystem
    """
    identifier = record.header.identifier  
    record_path = os.path.join(basepath, identifier) + ".pax"
    return record_path, identifier

def save_metadata_record(record:OAIItem, metadata_format:str, config: dict):
    """
    Save <metadata_format> metadata file which is associated 
    with record, onto disk. Out of date files are removed, 
    if they existed
    """
    pathjoin = os.path.join
    record_path, identifier = get_record_header_info(
            config["storage_directory"], record
        )
    metadata_file_path = pathjoin(record_path, 
            f"{identifier}.{metadata_format}")  
    
    with open(metadata_file_path, 'w', 
            encoding='utf-8') as f:
        log.info(f"writing file: {metadata_file_path}")
        tmp_tree = etree.fromstring(str(record))
        tmp_tree = tmp_tree.find("ns:metadata", namespaces={'ns': 'http://www.openarchives.org/OAI/2.0/'})
        tmp_tree = tmp_tree[0] # first child
        
        f.write(etree.tostring(tmp_tree).decode())  # Save as string for now
    


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
    print(xpath_expr)
    vals = tree.xpath(xpath_expr, namespaces=ns_dict)
    logger.debug(vals, [is_valid_url(x) for x in vals])
    return list(filter(is_valid_url, tree.xpath(xpath_expr, namespaces=ns_dict)))  

def get_default_http_client():
    # return an authenticated httpx client
    if request_client is not None:
        return request_client
    return httpx # placeholder: replace with authenticated client
        

def _retry_for_too_many_requests(client, url, retry_time = 1):
    # sleep first
    response_code = 429
    response = None
    while response_code == 429:
        time.sleep(retry_time)
        response = client.get(url)
        response_code = response.status_code     
    return response


def extract_filename(response):
    # prefer content-disposation if present, ortherwise use basename of original url
    headers = response.headers
    
    hd = headers["Content-Disposition"]
    logger.info(f"Content-Disposition value: {hd}")
    if hd is not None:
        filename = pyrfc6266.parse_filename(hd)
        logger.info(filename)
        return filename
    
    filename = os.path.basename(str(response.url))
    return filename

def get_web_resource(url) -> (str, str):
    client  = get_default_http_client()
    ret_text = None
    ret_filename = None
    try:
        response = client.get(uri)
        if response.status_code == 429:
            response = _retry_for_too_many_requests(client, uri)
        response.raise_for_status()
        ret_text = response.text
        ret_filename = extract_filename(response)
    except Exception as e:
        ret_text = None
        ret_filename = None
        logger.warn(f"failed to get {uri} due to {e}")
    return ret_text, ret_filename 

def save_representation_file(record:OAIItem, metadata_format:str, config:dict):
    """
    Extract URIs from metadata file in record, fetch files,
    and save results on disk
    """
    record_path, _ = get_record_header_info(config["storage_directory"], record)
    files_folder = os.path.join(record_path,
                                "Representation_Preservation")
    if not os.path.exists(files_folder):
        os.makedirs(files_folder, exist_ok = True)
    file_uris = extract_file_uris(record, config["fetch_xpath"],
                                  config["fetch_namespaces"])
    logger.info(file_uris)
    for uri in file_uris:
        logger.info(f"getting uri {uri}")
        uri_type = urlparse(uri).scheme
        text, file_name = None, None
        if uri_type in ('http', 'https'):        
            text, file_name = get_web_resoure(uri)
            if text is None:
                continue
        else:
            file_name = os.path.basename(uri)
            uri_path = UPath(uri, storage_options = config["upath-opts"][uri_type])
            if not uri_path.exists():
                logger.info("skipping {uri} due to upath failing to confirm it's existence")
                continue
            text = uri_path.read_text()
        file_path = os.path.join(files_folder, file_name)
        i = 1
        tmp_path = file_path
        while os.path.exists(tmp_path):
            base, ext = os.path.splitext(base)
            tmp_path = base + f"-{i}" + ext
            i += 1
        file_path = tmp_path
        with open(file_path, "w") as f:
            logger.info(f"saving {uri} @ {file_path}")
            f.write(response.text)
        


    
def get_identifiers(scythe, set_, config):
    identifiers = set()
    formats = scythe.list_metadata_formats()
    for mformat in formats:
        mprefix = mformat.metadataPrefix
        print(config["_from"], config["_until"])
        identifier_fetch = scythe.list_identifiers(
                metadata_prefix = mprefix,
                from_ = config["_from"],   
                until=config["_until"], set_ = set_)
        identifier_fetch = islice(identifier_fetch,
                             config["limit_entries"])
        for header in identifier_fetch:
            identifiers.add(header.identifier)
    return identifiers

def clear_existing_identifier(identifier, config):
    path = os.path.join(config["storage_directory"], identifier + ".pax")
    if os.path.exists(path):
        shutil.rmtree(path) 
        # some small chance of data loss, if fail happens in fetch of identifier documents
        # rmtree could be replaced with moving to a tmp folder which then gets removed on success
    os.makedirs(path, exist_ok=True)

def main():
    config = load_config("test.toml")
    preprocess_config(config)
    with Scythe(config["base_url"]) as scythe:
        authenticate_scythe(scythe, config["auth"])
        
        set_list = [None]

        if config.get("sets") is not None and len(config.get("sets")) > 0:
            set_list = config.get("sets")
        for set_ in set_list:
            # list identifiers
            identifiers = get_identifiers(scythe, set_, config)
            for identifier in identifiers:
                clear_existing_identifier(identifier, config)
                formats = scythe.list_metadata_formats(identifier)
                for mformat in formats:
                    mprefix = mformat.metadataPrefix
                    record = scythe.get_record(identifier, 
                                           mprefix)
                    save_metadata_record(record, mprefix, config)
                    if mprefix == config["metadata_format"]:
                        save_representation_file(record, mprefix, config)
                            
    if config["mode"] == "harvest":
        update_last_run(value = config["_until"])





if __name__ == '__main__':
    main()
