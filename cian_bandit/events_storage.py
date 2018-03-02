from cian_bandit.presto import set_username
from cian_bandit.presto import run_sql

from cian_bandit.metrics_sql import metrics_sql_columns
from cian_bandit.metrics_sql import metrics_sql
import pandas as pd
import json
import os


class NotDownloadedBatch(Exception):
    pass


class EventsStorage:
    def __init__(self, last_events_cnt):
        self.username = os.environ['PRESTO_USER']
        self.last_events_cnt = last_events_cnt

    def _download_batch(self):
        sql = metrics_sql(self.last_events_cnt)

        set_username(self.username)
        result = run_sql(sql, rs=True)

        if result is None or len(result) == 0:
            raise NotDownloadedBatch("presto didnt return data")

        batch = pd.DataFrame(result)
        batch.columns = metrics_sql_columns
        self.batch = batch

    def get_batch(self):
        self._download_batch()
        return self.batch
