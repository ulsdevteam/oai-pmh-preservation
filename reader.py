
#from model import OpexXmlHelper, OpexMetadataContent, OpexFileContent, OpexFolderContent
#from . import model
import model
import os
import os.path
import lxml.etree as etree


class Reader:

    def __init__(self, file_path:str, is_dir=None):
        self.file_path = file_path
        if is_dir is None:
            self.is_dir = os.path.isdir(file_path)
        # remove blank text for better writer output
        xml_parser = etree.XMLParser(remove_blank_text=True)
        self.tree = etree.parse(file_path, xml_parser)
        
    def get_contents(self) -> tuple[
            model.OpexMetadataContent, 
            model.OpexFileContent | model.OpexFolderContent]:
        metadata = model.OpexMetadataContent.from_xml(self.tree)
        fs_content = None
        if self.is_dir:
            fs_content = model.OpexFolderContent.from_xml(self.tree)
        else:
            fs_content = model.OpexFileContent.from_xml(self.tree)
            print(fs_content)
        return metadata, fs_content
    '''
    @staticmethod
    def read_to_dict(tree):
        return xmltodict.parse(tree)
        # iterating through tree would lead to 
        # checking many cases so just using find
        return_dict = dict()
        properties_el = OpexXmlHelper.find(tree, "Properties")
        transfer_el = OpexXmlHelper.find(tree, "Transfer")
        descriptive_metadata_el = OpexXmlHelper.find(tree, "DescriptiveMetadata")
        
        if properties_el is not None:
            properties_dict = dict()
            properties_dict['title'] = OpexXmlHelper.find(properties_el, "Title")
            properties_dict['description'] = OpexXmlHelper.find(properties_el, "Description")
            properties_dict['security_descriptor'] = OpexXmlHelper.find(
                                            properties_el, "SecurityDescriptor")
            properties_dict['identifiers'] = None
            identifiers_el = OpexXmlHelper.find(properties_el, "Identifiers")
            if identifiers_el is not None:
                properties_dict['identifiers'] = []
                for i in identifiers_el:
                    properties_dict['identifiers'].append(
                        (i.text, i.attrib)
                    )
            if filter_missing:
                for key, value in properties_dict.items():
                    if value is None:
                        properties_dict.pop(key)
                    elif isinstance(value, etree.ET):
                        properties_dict[key] = properties_dict[key].text
            return_dict['properties'] = properties_dict
        
        elif not filter_missing:
            return_dict['properties'] = None
        if transfer_el is not None:
            raise NotImplementedError
       ''' 
            
                        
                        
            
        
                                                        
            
        
        
