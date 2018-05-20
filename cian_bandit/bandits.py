import numpy as np
import pandas as pd


from cian_bandit.weights_storage import WeightStorage, handle_null_conversion


class WrongModelException(Exception):
    pass


class ZeroConversion(Exception):
    pass


class SimpleBandit(object):
    """
    На каждый page_type свой бандит
    В этой простейшей версии трафик распределяется пропорционально конверсиям
    Каждой модели отдаётся как минимум min_weight трафика
    j(t) = argmax (\mu_i)
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
        events_cnt = batch_data['shows'].sum()
        batch_dict = batch_data.to_dict(orient='records')
        batch_metrics = {}

        for d in batch_dict:
            batch_metrics[(d['page_type'], d['model_version'])] = d

        for model_version, model_batch in batch_data.groupby('model_version'):
            if model_version in self.models:
                this_batch_dict = batch_metrics[(self.page_type, model_version)]
                self.models[model_version].metric.add_batch(this_batch_dict, events_cnt)

    def _handle_null_weight(self, w):
        if w is None or pd.isnull(w):
            w = self.min_weight
        return w

    def _normalize_weights(self, metric_values):
        norm = sum(metric_values.values())
        if norm == 0:
            norm = 10e-6
        for model_version, model in self.models.items():
            metric_value = metric_values[model_version]
            model.weight = metric_value / norm
            model.weight = self._handle_null_weight(model.weight)

    def recalc_models_weights(self):
        metric_values = {
            model_version: handle_null_conversion(model.metric.value)
            for model_version, model in self.models.items()
        }
        mv = list(metric_values.values())
        print('previous weights ' + self.page_type, ':', mv)
        if None in mv or np.allclose(mv, 0.0):
            print("All conversions are zero")
            if self.page_type != 'desktop':
                raise ZeroConversion('no info about conversions!')

        self._normalize_weights(metric_values)
        metric_values = {}
        for model_version, model in self.models.items():
            corrected_weight = model.weight
            corrected_weight = max(self.min_weight, corrected_weight)
            metric_values[model_version] = corrected_weight

        self._normalize_weights(metric_values)
        mv = list(metric_values.values())
        print('new weights ' + self.page_type, ':', mv)


class UCBBandit(SimpleBandit):
    """
    Use UCB-1 upper bound after CTR evaluation
    j(t) = argmax (\mu_i + sqrt(2lnt / n_i))
    Where t is amount of passed rounds and n_i is how many times we chose this arm.
    In batch terms: t is last batch size and n_i is num_shows of model version
    for more info: https://www.cs.mcgill.ca/~vkules/bandits.pdf
    """

    def _ucb(self, batch_size, num_chosen):
        if batch_size == 0 or pd.isnull(batch_size):
            return 0.0
        return 2 * np.log(batch_size) / max([1, num_chosen])

    def recalc_models_weights(self):
        metric_values = {
            model_version: handle_null_conversion(model.metric.value)
            for model_version, model in self.models.items()
        }
        mv = list(metric_values.values())
        print('previous weights ' + self.page_type, ':', mv)
        if None in mv or np.allclose(mv, 0.0):
            print("All conversions are zero")
            if self.page_type != 'desktop':
                raise ZeroConversion('no info about conversions!')

        for model_version, model in self.models.items():
            conversion = handle_null_conversion(model.metric.value)
            ucb = self._ucb(
                model.metric.events_cnt,
                model.metric.shows,
            )

            print(model_version, 'ucb =', ucb)
            ucb_weight = conversion + ucb

            metric_values[model_version] = ucb_weight

        self._normalize_weights(metric_values)
        metric_values = {}
        for model_version, model in self.models.items():
            corrected_weight = model.weight
            corrected_weight = max(self.min_weight, corrected_weight)
            metric_values[model_version] = corrected_weight

        self._normalize_weights(metric_values)
        mv = list(metric_values.values())
        print('new weights ' + self.page_type, ':', mv)


class HeuristicBandit(SimpleBandit):
    """
    add memory for last N conversions and decay it's change
    add penalty to winner for too big p-value from ttest (means that it's highly likely that conversions are the same)
    that will make groups distribution more similar
    """
    pass

    # GOVNOKOD FROM JUPYTER:
    #
    # CRITICAL_PVALUE = 0.05
    # WEIGHTS_MEMORY_STEPS = 40
    #
    # MAX_TIME_COEFF = np.exp((len(convs_1) * 1. / 3.) / 30.) / 2.
    # weights_1, weights_2 = [], []
    # true_ws_1, true_ws_2 = [], []
    # glide_c1, glide_c2 = [], []
    # for i in range(1, len(convs_1)):
    #     try:
    #         lt_1 = np.mean([convs_1[max(0, i - 20):i]])
    #         lt_2 = np.mean([convs_2[max(0, i - 20):i]])
    #     except:
    #         continue
    #     glide_c1 += [lt_1]
    #     glide_c2 += [lt_2]
    #
    #     #     w1, w2 = convs_1[i], convs_2[i]
    #
    #     #     w1, w2 = lt_1, lt_2
    #
    #     w1 = np.exp(lt_1) / np.exp(lt_1 + lt_2)
    #     w2 = np.exp(lt_2) / np.exp(lt_1 + lt_2)
    #
    #     # memory for previous weights
    #     ltw_1 = [true_ws_1[max(0, i - WEIGHTS_MEMORY_STEPS):i]]
    #     ltw_2 = [true_ws_2[max(0, i - WEIGHTS_MEMORY_STEPS):i]]
    #     #     ltw_1 = weights_1[max(0,i-20):i]
    #     #     ltw_2 = weights_2[max(0,i-20):i]
    #
    #     norm = float(w1 + w2)
    #     true_ws_1 += [w1 / norm]
    #     true_ws_2 += [w2 / norm]
    #
    #     pvalue = 1.
    #     if i >= 20 and len(ltw_1) >= 1 and np.sum(np.isnan(ltw_1)) == 0:
    #         #         pass
    #         #         deltas = ltw_1 - ltw_2
    #         #         """
    #         #         This is a two-sided test for the null hypothesis that 2 related or
    #         #         repeated samples have identical average (expected) values.
    #         #         """
    #         #         sts.ttest_rel(ltw_1, ltw_2)
    #         # if p-value <= alpha -> sets are not equal -> add high penalty for weak group
    #         #         w1 += ltw_1 * 20.
    #         #         w2 += ltw_2 * 20.
    #         _, pvalue = sts.ttest_rel(ltw_1, ltw_2, axis=1)
    #         print
    #         pvalue, ' ',
    #         wg = who_greater(np.mean(ltw_1), np.mean(ltw_2))
    #         penalty = 1.
    #
    #         # add proof by time (i)
    #         if pvalue < CRITICAL_PVALUE:
    #             #             print(pvalue)
    #             #             penalty =
    #             # min([0.8 + (pvalue/0.05),1])
    #             # 10 is max coeff at the end
    #             time_coeff = np.exp(i / 20.) / 2.
    #             time_coeff = min(1., max(0., time_coeff / MAX_TIME_COEFF))
    #             penalty = penalty * (1. - time_coeff) \
    #                       + pvalue / 0.05 * time_coeff
    #         print
    #         penalty, ' ',
    #
    #         #             print(penalty)
    #         #             penalty = np.log(1.+pvalue/CRITICAL_PVALUE)#0.95
    #         #         if pvalue < 10e-6:
    #         #             penalty = 0.8
    #
    #         if wg == 'left':
    #             #             print '__w1 ',penalty,
    #             w2 = w2 * penalty
    #         elif wg == 'right':
    #             #             print 'w2 ', penalty,
    #             w1 = w1 * penalty
    #             # definetely someone is less
    #     print
    #     wg
    #     norm = float(w1 + w2)
    #     w1 = w1 / norm
    #     w2 = w2 / norm
    #
    #     # #     if i < 10:
    #     #         w1, w2 = .5,.5
    #     weights_1 += [w1]
    #     weights_2 += [w2]
    # NUM_LAST_WEIGHTS = 200
    #
    # def recalc_models_weights(self):
    #     ws = WeightStorage()
    #     previous_weights = ws.get_previous_weights(
    #         page_type=self.page_type,
    #         num=self.NUM_LAST_WEIGHTS,
    #     )
    #