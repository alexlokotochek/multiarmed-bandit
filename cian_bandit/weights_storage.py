from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import defaultdict
from datetime import datetime
import json


class NotFoundBanditConfig(Exception):
    pass


class NotUpdatedConfig(Exception):
    pass


class WeightStorage:
    """
    Elasticsearch storage
    doctype:
    {
        'weights': {
            'desktop': {
                '0': 0.4,
                '1': 0.6,
                'updated': '2017-11-05 00:01:02'
            },
            'mobile': {
                'updated': '2017-11-05 00:01:02'
            }
        }
    }
    """
    def __init__(self, hosts):
        self.es_client = Elasticsearch(hosts=hosts, maxsize=1)

    def get_last_config_doc(self):
        """
        config is stored as es doc with id=0
        will be overwritten every time
        :return: config json from es
        """
        last_result = self.es_client.get(
            index='bandit_pio',
            doc_type='bandit_config',
            id=0,
        )
        if not last_result['found']:
            raise NotFoundBanditConfig("No bandit config document")
        return last_result

    def update_page_type_models_weights(self, page_type, models):
        """
        :param models: list[Model]
        """
        previous_result = self.get_last_config_doc()

        if not previous_result['found']:
            raise NotFoundBanditConfig("Nothing to update in storage")

        doc = previous_result['_source']
        if page_type not in doc['weights']:
            doc['weights'][page_type] = {}
            
        doc['weights'][page_type] = {}
        doc['weights'][page_type]['updated'] = str(datetime.now())[:19]

        for model_version, model in models.items():
            doc['weights'][page_type][model_version] = model.weight

        doc['weights'] = dict(doc['weights'])

        index_result = self.es_client.index(
            index='bandit_pio',
            doc_type='bandit_config',
            id=0,
            body=doc,
        )

        previous_version = previous_result['_version']
        new_version = index_result['_version']
        
        if previous_version == new_version:
            raise NotUpdatedConfig("Couldnt update bandit config document")
