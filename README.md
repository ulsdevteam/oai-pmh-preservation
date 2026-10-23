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
lxml
pyrfc6266
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

1. setup `config.toml` (See `example.toml` for reference)

2. `python scythe.py` to fetch metadata and file contents

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

Once the HTTP client is sufficiently authenticated, the OAI-PMH endpoint is polled for identifiers between last run date (as stored in state.txt) and the current time (or `until`, if specified in config) using a GetIdentifers query. In fetch mode, the identifiers are fetched between the `from` and `until` parameters of config (If `from` or `until` are unspecified, the query parameters are not passed to the OAI-PMH query)
The corresponding query looks something similar to as follows

```
GET https://<OAI-PMH endpoint>/?verb=ListIdentifierss&metadataPrefix=oai_dc&from=<last_run_date>&until=<today>
```

`oai_dc` is the metadataPrefix in the GetIdentifiers as all OAI-PMH records are required by standard to expose an
`oai_dc` metadata format.

In harvest mode, if no `state.txt` is present in the filesystem, the from field is not added to the query parameters, and thus all records before `today` are fetched.

A `set_list` config parameter can be set, if it is desired that harvesting occur only from particular sets. In this case,
the set list is iterated through and identifiers from each set specified are processed.

For each identifier that appears as a response to the OAI-PMH query
1. For each format in a `ListMetadataFormats` query, a GetRecord query is called with query parmaeters Identifier, and MetadataPrefix set
2. The record XML (i.e. XML metadata) is saved within the filesystem, in the path `<storage_directory>/<identifier>.pax/<identifier>.<metadataPrefix>`, where
  - `storage_directory` is a config provided path
  - `identifier` is the OAI-PMH identifier for the record
  - `metadataPrefix` is the current metadata format being harvested

3. Web URLs representing the content the metadata is annotating are extracted from the metadata XML using a config provided XPATH 

4. The URL is fetched and stored in the same `identifier.pax` folder

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