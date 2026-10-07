## OAI-PMH to Preservica

Tool for periodically harvesting metadata and metadata-associated-files (generally, data that metadata is annotating)
from an [OAI-PMH](https://www.openarchives.org/pmh/) endpoint, and preparing it into an [OPEX](https://developers.preservica.com/documentation/open-preservation-exchange-opex)
package for upload into Preservica

## Setup
0.  Setup a virtual environment for dependencies
> `python -m venv .venv`
> `source .venv/bin/activate`
The name `.venv` can be chosen 

1. Install pip dependencies from requirements.txt
> `pip install -r requirements.txt`
The following packages will be installed
```
certifi
httpx==0.28.1
requests==2.32.3
truststore==0.10.1
oaipmh_scythe==0.13.0
opex-manifest-generator
tomlli
```
2. Install preservica-opex-parser
```
git clone https://github.com/ulsdevteam/preservica-opex-parser.git
cd preservica-opex-parser
pip install .
cd ..
```

## Quick Start

1. setup `config.txt` (See `example.toml` for reference)

2. `python scythe_refactor.py` to fetch metadata and file contents

3. `python generate_opex.py` to converted metadata and content fetch into opex package

## Usage Notes

There are 2 main stages for this application. A fetch/harvest stage and an opex generation phase. 

### Fetch/Harvest phase

The application begins by authenticating an http client, if authentication methods specified in config.

For authentication, one or more of the following are supported

1. `basic` : The HTTP client is provided a username and a password to authenticate with basic auth.

2. `cookie`: 
    2.1: A GET request is made to a login page. 
    2.2: The username and password fields of login form are extracted via xpaths (provided in config)
    2.3: A POST request is made to form action url, to submit the username and password provided in config.
    2.4: A header specified by the config (eg. "Set-Cookie") of the response is extracted. 
    2.5: The value extracted by previous step is set as a config provided field (eg. "Cookie") in the fetching HTTP client

Once the HTTP client is sufficiently authenticated, the OAI-PMH endpoint is polled for records between last run date (as stored in state.txt)
and today. The corresponding query looks something similar to as follows

```
GET https://<OAI-PMH endpoint>/?verb=ListRecords&metadataPrefix=<loop variable>&from=<last_run_date>&until=<today>
```

`loop variable` in the above refers to the fact that the following logic is repeated for every metadata format made available by the endpoint.

If no `state.txt` is present in the filesystem, the from field is not added to the query parameters, and thus all records before `today` are fetched.

For each record that appears as a response to the OAI-PMH query
1. The record XML (i.e. XML metadata) is saved within the filesystem, in the path `<storage_directory>/<identifier>.pax/<identifier>.<metadataPrefix>`, where
  - `storage_directory` is a config provided path
  - `identifier` is the OAI-PMH identifier for the record
  - `metadataPrefix` is the current metadata format being harvested

2. Web URLs representing the content the metadata is annotating are extracted from the metadata XML using a config provided XPATH 

3. The URL is fetched and stored in the same `identifier.pax` folder

Note that URL extraction only occours for a single config specified metadata format

Once all records have been processed, the file `state.txt` is updated (or created if one does not already exist) and set to the current date in (`YYYY-MM-DD`) format.

Following this the overall filesystem structure looks as follows

```
- storage_directory
  - identifier1.pax
    - Representation_Preservation
      - content_file1
      - content_file2
    - identifier1.format1
    - identifier1.format2
  - identifier2.pax
    ...
```

### Opex Phase

The goal of this phase is to transform the above storage directory structure into a valid pax structure

i.e. the resulting structure looks like this

```
- storage_dir
  - identifier1.pax
    - content_file1
    - content_file2
    - content_file1.opex
    - content_file2.opex
  - identifier2.pax
    ...
  - identifier1.pax.opex
  - identifier2.pax.opex
  ...
```

In a `content_file.opex` the following fields are set
- SourceID
- Fixities
- OriginalFilename

In an `identifier.pax.opex` the following fields are set
- Title, Description, SecurityDescriptor, Identifiers: Extracted from one of the metadata format files fetched
- DescriptiveMetadata: Metadata XML files are appended as subtrees to this tag. 

The metadata XML files are removed once the OPEX files have been generated