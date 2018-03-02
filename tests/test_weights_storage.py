from cian_bandit.presto import set_username
from cian_bandit.presto import run_sql

from cian_bandit.metrics_sql import metrics_sql_columns
from cian_bandit.metrics_sql import metrics_sql
import pandas as pd
import json
from cian_bandit.weights_storage import WeightStorage
from elasticsearch import Elasticsearch
import pytest
import os
from unittest import TestCase

from cian_bandit.bandits import SimpleBandit
from cian_bandit.bandits import ZeroConversion
from cian_bandit.metrics import ClickThroughRate
from cian_bandit.models import Model

EVENTS_PER_MODEL = 100000

os.environ['ES_HOSTS'] = 'hd-cli01.msk.cian.ru'


@pytest.fixture
def set_previous_config():
    hosts = os.environ['ES_HOSTS'].split(',')
    es = Elasticsearch(hosts=hosts, maxsize=1)
    prev_conf = {
        "weights": {
            "desktop": {
                "1": 0.499,
                "2": 0.501,
                "updated": "2017-12-04 03:10:47"
            },
            "mobile": {}
        }
    }
    result = es.index(
        index='bandit_pio',
        doc_type='bandit_config',
        id=0,
        body=json.dumps(prev_conf),
    )

    yield prev_conf

    es.delete(
        index='bandit_pio',
        doc_type='bandit_config',
        id=0,
    )


def test_previous_config(set_previous_config):
    prev_conf = set_previous_config

    hosts = os.environ['ES_HOSTS'].split(',')
    weights_storage = WeightStorage(hosts)
    last_config_doc = weights_storage.get_last_config_doc()

    assert prev_conf == last_config_doc['_source'], 'wrong previous config!'


def test_writing_config(set_previous_config):
    models = {}
    true_weights = {
        '1': 0.25,
        '2': 0.7,
        '3': 0.05
    }
    for model_version, weight in true_weights.items():
        metric = ClickThroughRate()
        model = Model(
            page_type='desktop',
            model_version=model_version,
            metric=metric,
            name=model_version,
        )
        model.weight = weight
        models[model_version] = model

    hosts = os.environ['ES_HOSTS'].split(',')
    storage = WeightStorage(hosts)

    storage.update_page_type_models_weights(
        page_type='desktop',
        models=models,
    )

    new_conf = storage.get_last_config_doc()

    for model_version, weight in true_weights.items():
        got_weight = new_conf['_source']['weights']['desktop'][model_version]
        assert weight == got_weight, 'wrote wrong weight'

