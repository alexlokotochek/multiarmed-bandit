import numpy as np


class ClickThroughRate(object):
    """
    Конверсия из показа в клик
    Чтобы не шумела, всегда считаем метрику по последним 30к
    (предполагается, что истинная конверсия не зависит от времени)
    Сглаживаем на smooth
    Любая группа будет гарантированно крутиться хотя бы на 5%,
    тогда, чтобы ей реабилитироваться, нужно будет несколько дней
    (всего в день ~100к событий)
    """
    def __init__(self, smooth=1000, min_confident_shows=10000):
        self.clicks = None
        self.shows = None
        self.smooth = smooth
        self.min_confident_shows = min_confident_shows
        self.value = None
        self.events_cnt = None

    def add_batch(self, batch_dict, events_cnt):
        """
        :param batch_dict: pd.DataFrame
            passed from Bandit with `metrics_sql_columns`
            required: 
                `clicks`
                `shows`
        :param events_cnt: int
            not used in simple CTR bandit
        """
        self.events_cnt = events_cnt
        self.clicks = batch_dict['clicks']
        self.shows = batch_dict['shows']
        self.shows += self.smooth
        self.value = self.clicks*1.0 / self.shows
