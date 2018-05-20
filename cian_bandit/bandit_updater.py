import json
from cian_bandit.bandits import SimpleBandit, UCBBandit
from cian_bandit.bandits import ZeroConversion
from cian_bandit.metrics import ClickThroughRate
from cian_bandit.models import Model
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

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
        self.page_types = [
            'mobile',
            'desktop',
        ]

    def init_bandits(self, config):
        self.bandits = []

        for page_type in self.page_types:
            model_versions = config[page_type]
            models = []
            for model_version, params in model_versions.items():
                metric = ClickThroughRate()
                model = Model(
                    page_type=page_type,
                    model_version=model_version,
                    metric=metric,
                    # todo: consistent names with pio engines jsons
                    name=model_version
                )
                models.append(model)

            bandit = UCBBandit(
                page_type=page_type,
                models=models,
                min_weight=0.05
            )
            self.bandits.append(bandit)

    def update_bandits(self, config):
        for bandit in self.bandits:
            try:
                bandit.evaluate_batch(self.batch)
                bandit.recalc_models_weights()
                if self.really_update_es:
                    pt_config = config[bandit.page_type]
                    self.weights_storage.update_page_type_models_weights(
                        page_type=bandit.page_type,
                        models=bandit.models,
                        page_type_config=pt_config,
                    )
                info_logger.info(bandit.page_type)
                info_logger.info(bandit.get_models_weights())
            except ZeroConversion as zc:
                logging.warning(bandit.page_type)
                logging.warning(zc)
