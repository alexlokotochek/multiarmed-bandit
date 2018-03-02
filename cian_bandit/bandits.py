import numpy as np


class WrongModelException(Exception):
    pass


class ZeroConversion(Exception):
    pass


class SimpleBandit(object):
    """
    На каждый page_type свой бандит
    В этой простейшей версии трафик распределяется пропорционально конверсиям
    Каждой модели отдаётся как минимум min_weight трафика
    """
    def __init__(self, page_type, models, min_weight):
        self.page_type = page_type
        self.min_weight = min_weight
        self.models = {}
        for model in models:
            if model.page_type != self.page_type:
                raise WrongModelException(
                    'Passed model with {0} page_type, {1} expected'.format(
                        model.page_type,
                        self.page_type
                    )
                )
            self.models[model.model_version] = model
       
    def get_models_weights(self):
        weights = {}
        for model_version, model in self.models.items():
            weights[model_version] = model.weight
        return weights
            
    def evaluate_batch(self, batch_data):
        """
        :param batch_data: pd.DataFrame
            aggtegated from pio_recs.sopr_shows
            grouped by page_type, model_version
            columns: page_type, model_version, clicked_cnt, phoned_cnt
        """
        batch_data = batch_data[batch_data['page_type'] == self.page_type]
        batch_dict = batch_data.to_dict(orient='records')
        batch_metrics = {}

        for d in batch_dict:
            batch_metrics[(d['page_type'], d['model_version'])] = d

        for model_version, model_batch in batch_data.groupby('model_version'):
            if model_version in self.models:
                this_batch_dict = batch_metrics[(self.page_type, model_version)]
                self.models[model_version].metric.add_batch(this_batch_dict)

    def _normalize_weights(self, metric_values):
        norm = sum(metric_values.values())
        for model_version, model in self.models.items():
            metric_value = metric_values[model_version]
            model.weight = metric_value / norm

    def recalc_models_weights(self):
        metric_values = {
            model_version: model.metric.value
            for model_version, model in self.models.items()
        }
        if np.allclose(list(metric_values.values()), 0.0):
            raise ZeroConversion("All conversions are zero")

        self._normalize_weights(metric_values)
        metric_values = {}
        for model_version, model in self.models.items():
            corrected_weight = model.weight
            corrected_weight = max(self.min_weight, corrected_weight)
            metric_values[model_version] = corrected_weight

        self._normalize_weights(metric_values)
