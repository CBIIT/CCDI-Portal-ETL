# CCDI Portal (aka Hub) ETL

## Data Loading

### Pre-requisites
Python 3.6 or newer

An Elasticsearch Service

### Add Dependencies
Run the following command to install dependencies.

```bash
pip3 install -r requirements.txt
```

Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in requirements.txt are listed below:

pyyaml

boto3

requests

### Configuration File
es_loader.yml

### Load Data into Elasticsearch
Run following command to load data into Elasticsearch:

```bash
python3 es_loader.py config/es_indices_bento.yml config/es_loader.yml
``` 

