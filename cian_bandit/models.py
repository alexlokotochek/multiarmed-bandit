class Model(object):
    def __init__(self, page_type, model_version, metric, name):
        self.page_type = page_type
        self.model_version = model_version
        self.metric = metric
        self.weight = None
        self.name = name

