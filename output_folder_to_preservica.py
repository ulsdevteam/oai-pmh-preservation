import os
from zipfile import ZipFile
def readConfigFile():
    # eventually we should switch to toml because the change for that would
    # just be quoted strings
    config = {}
    with open("config.txt", "r") as configFile:
        for line in configFile:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip()
    return config


def create_pax(folder_path, output_path):
    with ZipFile(output_path, 'x') as archive:
        
    

def main():
    # read config.txt
    conf = readConfigFile()
    input_folder = conf["storage-directory"]
    output_folder = input_folder + "_preservica"
    os.makedirs(output_folder, exist_ok=True)
    
    # start populating output_folder
         

if __name__ == "__main__":
    main()
