
# README #

# USAGE EXAMPLE:
```
alaktionov@hd-cli01:/home/alaktionov/alaktionov_oneshots/notebooks/multiarmed-bandit$ /anaconda3/bin/python bandit_updater.py
previous config: {
  "_index": "bandit_pio",
  "_type": "bandit_config",
  "_id": "0",
  "_version": 20,
  "found": true,
  "_source": {
    "weights": {
      "desktop": {
        "updated": "2017-12-04 03:10:44",
        "1": 0.49970331440196664,
        "2": 0.5002966855980334
      },
      "mobile": {}
    }
  }
}
desktop
{'1': 0.49970331440196664, '2': 0.5002966855980334}
mobile
All conversions are zero
new config: {
  "_index": "bandit_pio",
  "_type": "bandit_config",
  "_id": "0",
  "_version": 21,
  "found": true,
  "_source": {
    "weights": {
      "desktop": {
        "updated": "2017-12-04 03:10:47",
        "1": 0.49970331440196664,
        "2": 0.5002966855980334
      },
      "mobile": {}
    }
  }
}
```

