from cian_bandit.presto import set_username
from cian_bandit.presto import run_sql

from cian_bandit.metrics_sql import metrics_sql_columns
from cian_bandit.metrics_sql import metrics_sql
import pandas as pd
import json
from cian_bandit.weights_storage import WeightStorage
from cian_bandit.events_storage import EventsStorage
from elasticsearch import Elasticsearch
import pytest
import os
from unittest import TestCase

from cian_bandit.bandits import SimpleBandit
from cian_bandit.bandits import ZeroConversion
from cian_bandit.metrics import ClickThroughRate
from cian_bandit.models import Model

EVENTS_PER_MODEL = 100000

os.environ['LAST_EVENTS_CNT'] = str(10000)
os.environ['PRESTO_USER'] = 'alaktionov'

def test_events_storage():
    last_events_cnt = int(os.environ['LAST_EVENTS_CNT'])
    events_storage = EventsStorage(last_events_cnt)
    batch = events_storage.get_batch()
    # how to test db?
    assert len(batch) > 0 and batch['clicks'].sum() > 1000
