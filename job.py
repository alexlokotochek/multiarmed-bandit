import json
import os
import sys
import datetime
import argparse
from cian_bandit.weights_storage import WeightStorage
from cian_bandit.events_storage import EventsStorage
from cian_bandit.bandit_updater import BanditUpdater

os.environ['LAST_EVENTS_CNT'] = str(50000)
os.environ['ES_HOSTS'] = 'hdes01-data.cian.tech,hdes02-data.cian.tech,hdes03-data.cian.tech'
os.environ['PRESTO_USER'] = 'alaktionov'


def read_models_config():
    with open('./models_config.json', 'r') as f:
        conf = json.load(f)
    return conf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--update',
        type=int,
        help='True if update es bandit doc (affects production!)',
    )
    args = parser.parse_args()
    really_update_es = False
    if args.update is not None and args.update == 1:
        really_update_es = True
    print('Updating ES:', really_update_es)

    weights_storage = WeightStorage()
    last_config_doc = weights_storage.get_last_config_doc()
    print('previous config:', json.dumps(last_config_doc, indent=2))

    # todo: calc last_events_cnt for desired p-value and power
    last_events_cnt = int(os.environ['LAST_EVENTS_CNT'])
    events_storage = EventsStorage(last_events_cnt)
    batch = events_storage.get_batch()

    # hardcode desktop page_type because mobile data is not collected yet
    # todo: rm this
    batch['page_type'] = 'desktop'

    config = read_models_config()
    correct_models = config['desktop']
    batch = batch[batch['model_version'].isin(correct_models)]

    updater = BanditUpdater(
        weights_storage=weights_storage,
        batch=batch,
        really_update_es=really_update_es,
    )

    updater.init_bandits(config)
    updater.update_bandits(config)

    config = weights_storage.get_last_config_doc()
    print('new config:', json.dumps(config, indent=2))


if __name__ == '__main__':
    main()
