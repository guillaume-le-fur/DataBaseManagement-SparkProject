#!/home/guillaume/Documents/python-virtualenvs/spark-env/bin/python

import pandas as pd

from src.local_implementation.consts import Consts
from src.local_implementation.parent_classes.DataTransformer import DataTransformer


class IterateGrouper(DataTransformer):

    def __init__(self):
        super().__init__(Consts.ITERATE_SORT_OUTPUT_FILE, Consts.ITERATE_GROUP_OUTPUT_FILE)
        self.load_data()
        self.create_output()

    def group(self):
        self.output = pd.DataFrame(
            self.data.groupby('from')['to'].apply(lambda x: ', '.join(x.astype(str)))
        )
