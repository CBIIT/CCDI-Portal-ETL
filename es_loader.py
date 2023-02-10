#!/usr/bin/env python3
import argparse

import os
import yaml
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.session import Session

from bento.common.utils import get_logger, print_config

logger = get_logger('ESLoader')


class ESLoader:
    def __init__(self, es_host):
        if 'amazonaws.com' in es_host:
            awsauth = AWS4Auth(
                refreshable_credentials=Session().get_credentials(),
                region='us-east-1',
                service='es'
            )
            self.es_client = Elasticsearch(
                hosts=[es_host],
                http_auth = awsauth,
                use_ssl = True,
                verify_certs = True,
                connection_class = RequestsHttpConnection
            )
        else:
            self.es_client = Elasticsearch(hosts=[es_host])

    def create_index(self, index_name, mapping):
        """Creates an index in Elasticsearch if one isn't already there."""
        return self.es_client.indices.create(
            index=index_name,
            body={
                "settings": {"number_of_shards": 1},
                "mappings": {
                    "properties": mapping
                },
            },
            ignore=400,
        )

    def delete_index(self, index_name):
        return self.es_client.indices.delete(index=index_name, ignore_unavailable=True)

    def recreate_index(self, index_name, mapping):
        logger.info(f'Deleting old index "{index_name}"')
        result = self.delete_index(index_name)
        logger.info(result)

        logger.info(f'Creating index "{index_name}"')
        result = self.create_index(index_name, mapping)
        logger.info(result)

    def load_static_page(self, index_name, mapping, file_name):
        logger.info('Indexing content from about page')
        if not os.path.isfile(file_name):
            raise Exception(f'"{file_name} is not a file!')

        self.recreate_index(index_name, mapping)
        with open(file_name) as file_obj:
            about_file = yaml.safe_load(file_obj)
            for page in about_file:
                logger.info(f'Indexing static page "{page["page"]}"')
                self.index_data(index_name, page, f'page{page["page"]}')

    def index_data(self, index_name, object, id):
        self.es_client.index(index_name, body=object, id=id)



def main():
    parser = argparse.ArgumentParser(description='Load data into Elasticsearch')
    parser.add_argument('indices_file',
                        type=argparse.FileType('r'),
                        help='Configuration file for indices, example is in config/es_indices.example.yml')
    parser.add_argument('config_file',
                        type=argparse.FileType('r'),
                        help='Configuration file, example is in config/es_loader.example.yml')
    args = parser.parse_args()

    config = yaml.safe_load(args.config_file)['Config']
    indices = yaml.safe_load(args.indices_file)['Indices']
    print_config(logger, config)

    loader = ESLoader(
        es_host=config['es_host']
    )

    for index in indices:
        if index['type'] == 'about_file':
            if 'about_file' in config:
                loader.load_static_page(index['index_name'], index['mapping'], config['about_file'])
            else:
                logger.warning(f'"about_file" not set in configuration file, {index["index_name"]} will not be loaded!')
        else:
            logger.error(f'Unknown index type: "{index["type"]}"')
            continue


if __name__ == '__main__':
    main()
