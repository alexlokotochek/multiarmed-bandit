import json
from cian_bandit.bandits import SimpleBandit
from cian_bandit.bandits import ZeroConversion
from cian_bandit.metrics import ClickThroughRate
from cian_bandit.models import Model
import logging

import logging

info_logger = logging.getLogger('info_logger')
info_logger.setLevel(logging.INFO)


class MultipleBanditsPerPageType(Exception):
    pass


class BanditUpdater:
    def __init__(self, weights_storage, batch, really_update_es):
        self.weights_storage = weights_storage
        self.batch = batch
        self.really_update_es = really_update_es
        self.bandits = None

    def init_bandits(self, config):
        self.bandits = []

        for page_type, model_versions in config.items():
            models = []
            for model_version in model_versions:
                metric = ClickThroughRate()
                model = Model(
                    page_type=page_type,
                    model_version=model_version,
                    metric=metric,
                    # todo: consistent names with pio engines jsons
                    name=model_version
                )
                models.append(model)

            bandit = SimpleBandit(
                page_type=page_type,
                models=models,
                min_weight=0.05
            )
            self.bandits.append(bandit)

    def update_bandits(self):
        for bandit in self.bandits:
            try:
                bandit.evaluate_batch(self.batch)
                bandit.recalc_models_weights()
                if self.really_update_es:
                    self.weights_storage.update_page_type_models_weights(
                        page_type=bandit.page_type,
                        models=bandit.models,
                    )
            except ZeroConversion as zc:
                logging.warning(bandit.page_type)
                logging.warning(zc)
            else:
                info_logger.info(bandit.page_type)
                info_logger.info(bandit.get_models_weights())
