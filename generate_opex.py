
import lxml
import lxml.etree as etree
import opex_parser
import os
import tomllib as toml
def load_config(conf_path):
    # should be some table within conf.toml
    # but currently returning a default dict for testing

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
    d = os.path.basename(pax_path)
    print(d)
    parent = os.path.dirname(pax_path)
    # create pax opex
    fs_content = opex_parser.model.OpexFolderContent.from_pax_fs(
        os.path.join(root, d) 
        )
    writer = opex_parser.Writer(os.path.join(root, d) + ".opex", is_dir = True)
    writer.write(general_metadata, fs_content)
    
def extract_general_metadata(tree, conf, source_id):
    # extract title
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
    model_var = opex_parser.model.OpexMetadataContent(**metadata_dict,
                                            identifier_types =
                                            None, 
                                            source_id = source_id)
    return model_var

def create_subfile_opexes(path, files_folder, identifier):
    files_path, _, content_files = next(os.walk(os.path.join(path,files_folder)))
    for file in content_files:
        if file.endswith("opex"):
            continue
        stub = opex_parser.model.OpexMetadataContent(title = None,
                                                     description = None, 
                        source_id = identifier + "/" + file)
        fs_content = opex_parser.model.OpexFileContent.from_fs(
                os.path.join(files_path, file), ['SHA-1'])
        writer = opex_parser.Writer(os.path.join(files_path, file) + ".opex", is_dir = False)
        writer.write(stub, fs_content)

if __name__ == '__main__':
    # this is just 10 lines or something right?
    conf = load_config("test.toml")

    # folder structure := root -> ID -> metadata; files -> objects
    it = os.walk(conf["storage_dir"], topdown=True)
    root, dirs, _ = next(it) # skip root
    for d in dirs:
        identifier = os.path.basename(d)[:-len(".pax")]
        path, folders, files = next(os.walk(os.path.join(root, d)))
        metadata_trees = []
        model_var = None
        for file in files:
            if file.endswith("opex"):
                continue
            tree = etree.parse(os.path.join(path, file))
            if file.endswith(conf["metadata_format"]):
                model_var = extract_general_metadata(tree, conf, 
                                                     source_id=identifier)

            metadata_trees.append(tree)
        for tree in metadata_trees:
            model_var.append_descriptive_metadata(tree)

        create_subfile_opexes(path, "Representation_Preservation", identifier)
        
        write_pax_opex(os.path.join(root, d), model_var)




    
    print(conf)


    
