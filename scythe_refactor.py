
import os
import urllib3
import logging
from itertools import islice

try:
    import tomllib
except ImportError:
    try:
        import tomlli as tomllib
    except:
        logging.err("toml library not found")
        raise SystemExit(1)
logger = logging
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
        used_default = {(k,v) for k, v in defaults
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


def authenticate_scythe(client):
    return client

def fetch_metadata_records(scythe_client, metadata_format, 
    from_ = None, to_ = None):
    records = scythe_client.list_records(
        metadata_prefix = metadata_format.format_prefix) 
    
    records = islice(records, config["limit_entries"])
    return records
  
def save_metadata_records(records, metadata_format, config):
    pathjoin = os.path.join
    for record in records:
        identifier = record.header.identifier  
        identifier = identifier.replace(":", "_")
        record_path = pathjoin(config["storage_directory"],
                            identifier)
        
        metadata_file_path = pathjoin(record_path, 
                f"{identifier}.{metadata_format}")  
        if not os.path.exists(record_path):
            os.makedirs(record_path, exist_ok = True)
        with open(metadata_file_path, 'w', 
                encoding='utf-8') as f:
            f.write(str(record))  # Save as string for now
    
def save_metadata_files():
    raise NotImplementedError
def main():
    config = load_config()
    preprocess_config(config)
    with Scythe(config["base_url"]) as scythe:
        formats = scythe.list_metadata_formats()
        for meta_format in formats:
            records = fetch_metadata_records(
                            scythe, 
                            meta_format,
                            from_ = config["last_run"],
                            to_ = config["today"])


if __name__ == '__main__':
    main()
