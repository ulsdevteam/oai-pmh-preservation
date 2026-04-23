
import copy
from dataclasses import dataclass, fields
from typing import Optional, Self, NamedTuple
import lxml.etree as etree
import os
from lxml.builder import ElementMaker
import hashlib

class MetadataContentOpexFragments(NamedTuple):
    title: Optional[etree._Element] 
    description: Optional[etree._Element] # <Description> ... </Description>
    source_id: Optional[etree._Element]
    security_descriptor: etree._Element # <SecurityDescriptor>...</Sec...>
    identifiers: Optional[etree._Element] 
    descriptive_metadata: Optional[etree._ElementTree]
    pass

class FileContentOpexFragments(NamedTuple):
    original_filename: Optional[etree._Element]
    fixities: Optional[etree._Element]

class FolderContentOpexFragments(NamedTuple):
    original_filename: Optional[etree._Element]
    manifest: Optional[etree._Element]

    

@dataclass
class OpexMetadataContent:
    # a class to represent the content within an opex file independent
    # of xml
    title: Optional[str]
    description: Optional[str]
    source_id: str
    security_descriptor: str = ""
    identifiers: list[Optional[str]] = None
    identifier_types: list[Optional[str]] = None
    descriptive_metadata: etree._ElementTree = None
    

    def as_xml_fragments(self: Self) -> MetadataContentOpexFragments:
        builder = OpexXmlHelper.opex_builder
        named_tuple_fields = [field.name for field in fields(self)]
        #print(named_tuple_fields)
        named_tuple_fields.remove("identifier_types")
        #ret_type = namedtuple("Fragments", named_tuple_fields)
        ret_type = MetadataContentOpexFragments
        ret = [None]*len(named_tuple_fields)
        
        for i, field in enumerate(named_tuple_fields):
            field_val = getattr(self, field)
            print(field, field_val)
            if field_val is None:
                continue
            match field:
                case "title":
                    ret[i] = builder("Title")
                    ret[i].text = field_val
                case "description":
                    ret[i] = builder("Description")
                    ret[i].text = field_val
                case "source_id":
                    ret[i] = builder("SourceID")
                    ret[i].text = field_val
                case "security_descriptor":
                    ret[i] = builder("SecurityDescriptor")
                    ret[i].text = field_val
                case "identifiers":
                    ret[i] = builder("Identifiers")
                    id_types = self.identifier_types
                    if id_types is None:
                        id_types = [None]*len(self.identifiers)
                        
                    for value, id_type in zip(self.identifiers,
                            id_types):
                        id_tag = builder("Identifier")
                        id_tag.text = value
                        if id_type is not None:
                            id_tag.set("type", id_type)
                        ret[i].append(id_tag)
                        
                case "descriptive_metadata":
                    ret[i] = field_val
        return ret_type._make(ret)
        
    def append_descriptive_metadata(self, subtree):
        subtree = copy.deepcopy(subtree)
        if self.descriptive_metadata is None:
            self.descriptive_metadata = (
                OpexXmlHelper.opex_builder("DescriptiveMetadata"))
        if isinstance(subtree, etree._ElementTree):
            subtree = subtree.getroot()
        self.descriptive_metadata.append(subtree)
    @classmethod
    def from_xml(cls, tree):
        title = OpexXmlHelper.find(tree, "Properties/Title")
        if title is not None:
            title = title.text
        description = OpexXmlHelper.find(tree, "Properties/Description")
        if description is not None:
            description = description.text
        source_id = OpexXmlHelper.find(tree, "Transfer/SourceID")
        if source_id is not None:
            source_id = source_id.text
        security_descriptor = OpexXmlHelper.find(tree, "Properties/SecurityDescriptor")
        if security_descriptor is not None:
            security_descriptor = security_descriptor.text
        
        descriptive_metadata = OpexXmlHelper.find(tree, "DescriptiveMetadata")
        identifiers = []
        identifier_types = []
        identifiers_el = OpexXmlHelper.find(tree, "Properties/Identifiers")
        if identifiers_el is None:
            identifiers_el = []
            
        for identifier_el in identifiers_el:
            identifiers.append(identifier_el.text)
            identifier_types.append(identifier_el.get("type"))
        return cls(title, description,  source_id, 
                security_descriptor, identifiers, 
                identifier_types, descriptive_metadata)
        
@dataclass
class OpexFileContent:
    # a class to represent opex content associated with 
    # file existing within filesystem
    original_filename: str
    fixity_algs: list[str]
    fixity_values: list[str]
    fixity_paths: Optional[list[str]] = None
    describes_pax:bool = False
    
    @classmethod
    def from_fs(cls, file_path, fixity_algs):
        assert os.path.exists(file_path) and os.path.isfile(file_path)
        filename = os.path.basename(file_path)
        alg_dict = {
            "SHA-1": hashlib.sha1,
            "SHA-256": hashlib.sha256,
            "SHA-512": hashlib.sha512,
            "MD5": hashlib.md5,
        }
        hash_names = []
        hash_values = []
        with open(file_path, "rb") as f:
            content = f.read()
        for alg in fixity_algs:
            assert alg in alg_dict
            digest = alg_dict[alg](content).hexdigest()
            hash_names.append(alg)
            hash_values.append(digest)
        return cls(filename, hash_names, hash_values)
    
    
    @classmethod
    def from_fs_pax(self, pax_file, pax_file_list):
        """ Unzip pax file at `pax_file` and generate paths and
        fixities for files within zipped pax object"""
        assert pax_file.endswith(".pax.zip")
        raise NotImplementedError()

    def as_xml_fragments(self) -> FileContentOpexFragments:
        builder = OpexXmlHelper.opex_builder
        named_tuple_fields = ["original_filename", "fixities"]
        ret_type = FileContentOpexFragments
        #ret_type = namedtuple("Fragments", named_tuple_fields)
        
        ret = [None]*len(named_tuple_fields)
        if self.original_filename is not None:
            ret[0] = builder("OriginalFilename")
            ret[0].text = self.original_filename
        
        ret[1] = builder("Fixities")
         
        for i, (alg, value) in enumerate(zip(self.fixity_algs,
                                             self.fixity_values)):
            fixity_tag = builder("Fixity")
            fixity_tag.set("type", alg)
            fixity_tag.set("value", value)
            if self.describes_pax:
                fixity_tag.set("path", self.fixity_paths[i])
            ret[1].append(fixity_tag)
        return ret_type._make(ret)

    @classmethod
    def from_xml(cls, tree):
        filename = OpexXmlHelper.find(tree, "Transfer/OriginalFilename")
        if filename is not None:
            filename = filename.text
        fixities_element = OpexXmlHelper.find(tree, "Transfer/Fixities")
        hash_names = []
        hash_values = []
        if fixities_element is None:
            return cls(filename, hash_names, hash_values)
        for fixity_el in fixities_element:
            hash_names.append(fixity_el.get("type"))
            hash_values.append(fixity_el.get("value"))
        return cls(filename, hash_names, hash_values)
            
@dataclass
class OpexFolderContent:
    original_filename: str
    subfolder_names: list[str]
    
    subfile_names: list[str]
    subfile_sizes: list[int]
    subfile_types: list[str]
    describes_pax:bool = False
    
    # todo: check if original filename is stored with folder opex
    def as_xml_fragments(self) -> FolderContentOpexFragments:
        builder = OpexXmlHelper.opex_builder
        ret_type = FolderContentOpexFragments
        #ret_type = namedtuple("Fragment", ["original_filename", "manifest"])
        ret = [None, None]
        if self.original_filename is not None:
            ret[0] = builder("OriginalFilename")
            ret[0].text = self.original_filename
        root = builder("Manifest")
        folder_root = builder("Folders")
        for folder_name in self.subfolder_names:
            folder_tag = builder("Folder")
            folder_tag.text = folder_name
            folder_root.append(folder_tag)

        files_root = builder("Files")
        for filename, size, ftype in zip(self.subfile_names, 
                                        self.subfile_sizes, 
                                        self.subfile_types):

            file_tag = builder("File")
            file_tag.text = filename
            file_tag.set("size", str(size))
            file_tag.set("type", ftype)
            files_root.append(file_tag)

        root.append(folder_root)
        root.append(files_root)
        ret[1] = root
        return ret_type._make(ret)
        pass

    @classmethod
    def from_fs(cls, file_path, skip_patterns=None):
        root, subdirs, subfiles = next(os.walk(file_path))
        if skip_patterns is not None:
            matches_patterns = lambda x: any(ptrn in x for ptrn in skip_patterns) 
            subdirs = list(filter(lambda x: not matches_patterns(x), subdirs))
            subfiles = list(filter(lambda x: not matches_patterns(x)), subfiles)
        
        subfile_sizes = []
        subfile_types = []
        for f in subfiles:
            size = os.stat(os.path.join(root, f)).st_size
            filetype = "metadata" if f.endswith(".opex") else "content"
            subfile_sizes.append(size)
            subfile_types.append(filetype)
            
        return cls(file_path, subdirs, subfiles, subfile_sizes, subfile_types)
    
    @classmethod
    def from_fs_pax(cls, base_path, file_paths, folder_paths):
        # let user tell me paths
        # currently only look for representation_preservation
        subfolder_names = []
        subfile_names = []
        subfile_types = []
        subfile_sizes = []
        assert os.path.isdir(base_path)
        for path in folder_paths:
            full_path = os.path.join(base_path, path)
            assert os.path.exists(full_path)
            subfolder_names.append(path)

        for path in file_paths:
            full_path = os.path.join(base_path, path)

            assert ps.path.exists(full_path)
            size = os.stat(full_path).st_size
            filetype = "metadata" if path.endswith(".opex") else "content"
            subfile_names.append(path)
            subfile_types.append(filetype)
            subfile_sizes.append(size)


        return cls(None, subfolder_names, subfile_names, 
                   subfile_sizes, subfile_types)
                
            
    @classmethod
    def from_xml(cls, tree):
        
        # look for manifest
        manifest_el = OpexXmlHelper.find(tree, "Transfer/Manifest")
        if manifest_el is None:
            return None
        
        file_path = OpexXmlHelper.find(tree, "Transfer/OriginalFilename")
        if file_path is not None:
            file_path = file_path.text
        # find all Manifest/Folders/Folder and store text of each folder
        folders_el = OpexXmlHelper.find(manifest_el, "Folders")
        folders_el = folders_el if folders_el is not None else []
        subfolder_names = []
        for folder_el in folders_el:
            subfolder_names.append(folder_el.text)
        files_el = OpexXmlHelper.find(manifest_el, "Files")
        files_el = folders_el if files_el is not None else []

        # Find all Manifest/Files/File and store properties
        subfile_names = []
        subfile_sizes = []
        subfile_types = []
        for file_el in files_el:
            subfile_names.append(file_el.text)
            subfile_sizes.append(int(file_el.get("size")))
            subfile_types.append(file_el.get("type"))
        
        return cls(file_path, subfolder_names, subfile_names, subfile_sizes, subfile_types)

class OpexXmlHelper:
    # utility class to generate xml fragments and 
    # query Opex xml files
    OPEX_NS  = "http://www.openpreservationexchange.org/opex/v1.2"
    ns_dict = {'opex': OPEX_NS} # convert to frozendict in 3.15
    opex_builder = ElementMaker(
        namespace=OPEX_NS,
        nsmap = ns_dict
    )
    
    @staticmethod
    def find(tree, name):
        # prepend_ns
        name = '/'.join('opex:' + x for x in name.split('/'))
        tag = tree.find(name, namespaces = OpexXmlHelper.ns_dict)
        if tag is None:
            print(tag)
        return tag
    
    
    
   
    
