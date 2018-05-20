from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import defaultdict
from datetime import datetime
import json
import pandas as pd
import time
import os


def handle_null_conversion(c):
    if c is None or pd.isnull(c):
        c = 10e-6
    return c


class NotFoundBanditConfig(Exception):
    pass


class NotUpdatedConfig(Exception):
    pass


class WeightStorage:
    """
    Elasticsearch storage
    doctype:
    {
        'mobile': {},
        'desktop': {
            '7': {
                'prob': 0.51,
                'parameters': {
                    'es_index': 'item_correlations_desktop_alias',
                    'must_not_fields': ['phone'],
                    'item_boost': 1.0,
                    'last_history_views': 20,
                    'last_history_phones': 20
                }
            },
            '8': {
                'prob': 0.49,
                'parameters': {
                    'es_index': 'item_correlations_desktop_alias',
                    'must_not_fields': ['view', 'phone'],
                    'item_boost': 1.0,
                    'last_history_views': 5,
                    'last_history_phones': 5
                }
            }
        },
        'updated': '2018-03-20 00:22:33',
        'config_version': 1
    }
    """
    def __init__(self, hosts=None):
        if hosts is None:
            hosts = os.environ['ES_HOSTS'].split(',')
        self.es_client = Elasticsearch(hosts=hosts, maxsize=1)

    def get_last_config_doc(self):
        """
        config is stored as es doc with id=0
        will be overwritten every time
        :return: config json from es
        """
        last_result = self.es_client.search(
            index='bandit_ml_recs',
            doc_type='bandit_config',
            sort='config_version:desc',
        )['hits']['hits']

        if len(last_result) == 0:
            raise NotFoundBanditConfig("No bandit config document")

        last_result = last_result[0]
        return last_result

    def get_previous_weights(self, page_type, num):
        """
        Get list of last num weights per every page_type->model_version
        """
        last_result = self.es_client.search(
            index='bandit_ml_recs',
            doc_type='bandit_config',
            sort='config_version:desc',
            size=num,
        )['hits']['hits']

        if len(last_result) == 0:
            raise NotFoundBanditConfig("No bandit config document")

        weights = defaultdict(list)
        for doc in last_result:
            pt_doc = doc[page_type]
            for model_version, info in pt_doc.items():
                prob = info['prob']
                weights[model_version] += [prob]
        return weights

    def update_page_type_models_weights(self, page_type, models, page_type_config):
        """
        :param models: list[Model]
        """
        if page_type != 'desktop':
            # still cant update mobile!
            return
        previous_result = self.get_last_config_doc()
        previous_version = int(previous_result['_source']['config_version'])
        doc = dict(previous_result['_source'])
        doc[page_type] = page_type_config
        for model_version, model in models.items():
            doc[page_type][model_version]['prob'] = model.weight
            conversion = handle_null_conversion(model.metric.value)
            doc[page_type][model_version]['conversion'] = conversion
        doc['updated'] = str(datetime.now())[:19]
        doc['config_version'] = int(previous_result['_source']['config_version']) + 1
        doc = dict(doc)
        self.es_client.index(
            index='bandit_ml_recs',
            doc_type='bandit_config',
            body=doc,
        )
        # wait for config to be indexed
        time.sleep(3)
        new_version = self.get_last_config_doc()['_source']['config_version']

        if previous_version == new_version:
            print(previous_version, 'vs', new_version)
            raise NotUpdatedConfig("Couldnt update bandit config document")
