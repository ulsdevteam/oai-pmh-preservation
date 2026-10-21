
from lock import create_lock_file, release_lock_file
import lxml
import lxml.etree as etree
import opex_parser
import os
import tomllib as toml

def load_config(conf_path):
    """
    Load and preprocess config. See example.toml for config details. 
    """

    default = {
        "storage_dir": "oaipmh-storage",
        "metadata_format": "oai_dc",
        "xpaths": {
            "title": "//dc:title/text()",
            "description": "//dc:description/text()",
            "identifiers": "//dc:identifier/text()",

        },
        "ns": {
            "opex": "http://www.openpreservationexchange.org/opex/v1.2",
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "dc":"http://purl.org/dc/elements/1.1/"
        }
    }
    if conf_path is None:
        return default
    else:
        with open(conf_path, 'rb') as f:
            return toml.load(f)["opex"]


def write_pax_opex(pax_path, general_metadata):
    """
    Wrapper to opex_writer.write, with Descriptive Metadata from `general_metadata`
    and filesystem metadata extracted from pax_path directory. 

    In context, this is used to create identifier.pax.opex using metadata from general_metadata
    and filesystem metadata from folder.
    """
    d = os.path.basename(pax_path)
    parent = os.path.dirname(pax_path)
    # create pax opex
    fs_content = opex_parser.model.OpexPaxContent.from_fs(
        os.path.join(root, d), [opex_parser.model_types.HashAlgorithm.sha1]
        )
    writer = opex_parser.Writer(os.path.join(root, d) + ".opex", is_dir = True, is_pax = True)
    writer.write(general_metadata, fs_content)
    
def extract_general_metadata(tree, conf, source_id):
    """
    Apply xpaths in dictionary `conf` to specified lxml etree `tree`
    to extract relevant fields from tree and return a corresponding OpexMetadataContent object
    """
    metadata_dict = {}
    single_items = ["title", "description", "source_id",
                    "security_descriptor"]
    for key, val in conf["xpaths"].items():
        if key == "source_id":
            continue
        res = tree.xpath(val, namespaces=conf["ns"])
        print(f"Getting xpath for {key}:{val} = {res}")
        if key in single_items and isinstance(res, list):
            if len(res) == 0:
                res = None
            else:
                res = res[0]
        metadata_dict[key] = res
    model_var = opex_parser.model.OpexMetadataContent(
                                            title = metadata_dict["title"],
                                            description = metadata_dict["description"],
                                            source_id = source_id,
                                            identifiers = [opex_parser.model_types.CompoundTypes.Identifier(value = x, type = None) for x in metadata_dict["identifiers"]],
                                            descriptive_metadata = None)
    return model_var

def create_subfile_opexes(path, files_folder, identifier):
    """
    Create an opex for each file within the pax folder
    """
    #print(path, files_folder)
    if not os.path.exists(os.path.join(path, files_folder)):
        print(f"no artifacts found in {os.path.join(path, files_folder)}")
        return 
    files_path, _, content_files = next(os.walk(os.path.join(path,files_folder)))
    
    for file in content_files:
        if file.endswith("opex"):
            continue
        stub = opex_parser.model.OpexMetadataContent(
                        title = None,
                        description = None, 
                        source_id = identifier + "/" + file, 
                        identifiers = [],
                        descriptive_metadata = None)
        fs_content = opex_parser.model.OpexFileContent.from_fs(
                os.path.join(files_path, file), ['SHA-1'])
        writer = opex_parser.Writer(os.path.join(files_path, file) + ".opex", is_dir = False, is_pax=False)
        writer.write(stub, fs_content)

def delete_existsing_opex(path):
    """
    Recursively find opex files within directory at `path` and remove them.
    """
    for root, _, file in os.walk(path):
        if file.endswith("opex"):
            os.remove(os.path.join(root, file))

if __name__ == '__main__':
    """
    Main loop
    For each of the identifier folders in `conf["storage_dir"]`, create the following
    - identifier.pax.opex (call to write_pax_opex)
    - file.opex for each internal file in identifier.pax folder (call to create_subfile_opexes).

    Metadata is extracted from metadata file identifier.pax/identifier.{format} where format is conf["metadata_format"]
    and used to populate identifier.pax.opex, using xpaths from conf

    All metadata files in `identifier.pax` folder are added to descriptive metadata of generated 
    identifier.pax.opex.

    Additional features are:
    - Concurrency control: A lock file checks if scythe.py or another instance of generate_opex.py are modifying
      the conf["storage_dir"]. If so, script exits to prevent conflict.
    - Accumulative: Skips any idenitifer.pax directory that has already been processed
    - Error Recovery: If a generate_opex.py run ends prematurely with an error, the opex files for the identifier
      that was being edited when script failed are regenerated.
    """
    conf = load_config("test.toml")
    logging.debug(f"Using conf {conf}")
    lockfile_path = os.path.join(conf["storage_dir"], ".modify_lock")
    if not create_lock_file(lockfile_path):
        logging.error("Lock File found, another program is currently modifying the directories...")
        raise SystemExit(1)
    # folder structure: root -> Representation_Preservation, metadata_files
    try:
        it = os.walk(conf["storage_dir"], topdown=True)
        root, dirs, _ = next(it) # skip root
        for d in dirs:
            identifier = os.path.basename(d)[:-len(".pax")]
            path, folders, files = next(os.walk(os.path.join(root, d)))
            mark_for_deletion = []
            # if a folder has no root/metadata_files, this folder has only been processed
            if len(files) == 0:
                continue
            # attempt to create a lock file for identifier
            # used for recovery in case of failure mid run, with partially written opex
            if not create_lock_file(os.path.join(path, "GENERATING_OPEX")):
                # recovery needed, remove all existing opex files in identifier dir since they are in an incomplete state
                delete_existsing_opex(path)
                # remove stray GENERATING_OPEX from files
                files = [f for f in files if f != "GENERATING_OPEX"]
                # folder is in clean state and opex can be regenerated

            metadata_trees = []
            model_var = None
            model_tree = None
            for file in files:
                if file.endswith("opex"):
                    continue
                file_path = os.path.join(path, file)
                tree = etree.parse(os.path.join(path, file))
                if file.endswith(conf["metadata_format"]):
                    model_tree = tree
                metadata_trees.append(tree)
                mark_for_deletion.append(file_path)
                        
            model_var = extract_general_metadata(model_tree, conf, 
                                                     source_id=identifier)

        
            for tree in metadata_trees:
                model_var.append_descriptive_metadata(tree)
            
            create_subfile_opexes(path, "Representation_Preservation", identifier)
        
            write_pax_opex(os.path.join(root, d), model_var)

            release_lock_file(os.path.join(path, "GENERATING_OPEX"))
            for file_path in mark_for_deletion:
                os.remove(file_path)
    finally:
        release_lock_file(lockfile_path)




    
   


    
