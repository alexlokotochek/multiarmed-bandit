from cian_bandit.presto import set_username
from cian_bandit.presto import run_sql

from cian_bandit.metrics_sql import metrics_sql_columns
from cian_bandit.metrics_sql import metrics_sql
import pandas as pd
import json
from cian_bandit.weights_storage import WeightStorage
from cian_bandit.bandit_updater import BanditUpdater
from elasticsearch import Elasticsearch
import pytest
import os
from scipy.stats import poisson
from scipy.stats import bernoulli
import logging
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


def test_bandit_updater(set_previous_config):
    """
    users come with pois(lambda) intensity every .rvs milliseconds
    llh evaluation of lambda is its expectation
    assuming average rps for each model_version is 10 => model.weight*1000/10 is lambda
    """

    hosts = os.environ['ES_HOSTS'].split(',')
    weights_storage = WeightStorage(hosts)
    last_config_doc = weights_storage.get_last_config_doc()
    previous_weights = last_config_doc['_source']['weights']['desktop']
    logging.info(str(previous_weights) + 'previous weights')

    last_weights = {
        '1': 0.25,
        '2': 0.7,
        '3': 0.05,
    }

    # only CTR
    true_conversions = {
        '1': 0.02,
        '2': 0.025,
        '3': 0.03,
    }

    model_info = {}
    for model_version, model_weight in last_weights.items():
        lambda_ = (1./model_weight)
        users_moments_diffs = poisson.rvs(
            lambda_ * 100.,
            size=24 * 3600 * 10
        )
        cur_time = 0.
        visits = 0
        while cur_time < 24 * 3600 * 1000 and visits < len(users_moments_diffs):
            cur_time += users_moments_diffs[visits]
            visits += 1
        model_info[model_version] = {}
        model_info[model_version]['shows'] = visits

        true_conversion = true_conversions[model_version]
        clicks = bernoulli.rvs(true_conversion, size=visits)
        model_info[model_version]['clicks'] = sum(clicks)
        real_conversion = sum(clicks)*1.0/visits
        model_info[model_version]['real_conversion'] = real_conversion
        model_info[model_version]['true_conversion'] = true_conversion

    rows = []
    for model_version, info in model_info.items():
        clicks, shows = info['clicks'], info['shows']
        row = [model_version, clicks, shows]
        rows += [row]

    batch = pd.DataFrame(rows)
    batch.columns = ['model_version', 'clicks', 'shows']
    logging.info(str(model_info))

    models_config = {
      "desktop": ["1", "2", "3"],
      "mobile": []
    }

    batch['page_type'] = 'desktop'

    updater = BanditUpdater(weights_storage, batch, True)
    updater.init_bandits(models_config)
    updater.update_bandits()

    new_config_doc = weights_storage.get_last_config_doc()
    new_weights = new_config_doc['_source']['weights']['desktop']
    logging.info(str(new_weights) + 'new weights')

    # first <= second <= third
    assert new_weights['1'] < new_weights['2'] < new_weights['3'], 'wrong order after evaluation!'
