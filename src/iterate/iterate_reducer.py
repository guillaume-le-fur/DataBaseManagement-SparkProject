#!/home/guillaume/Documents/python-virtualenvs/spark-env/bin/python

from src.consts import Consts
from src.parent_classes.DataTransformer import DataTransformer


class IterateReducer(DataTransformer):

    def __init__(self):
        super().__init__(Consts.ITERATE_GROUP_OUTPUT_FILE, Consts.ITERATE_REDUCER_OUTPUT_FILE)
        self.load_data()
        self.create_output()
        self.counter = 0

    def reduce(self):
        for index, row in self.data.iterrows():
            key = int(row['from'])
            val = row['to'].rstrip('\n')
            values = [int(v) for v in val.split(',')]
            min_k = key
            value_list = []
            for val in values:
                if val < min_k:
                    min_k = val
                value_list.append(val)
            if min_k < key:
                self._add_output(key, min_k)
                for val in value_list:
                    if min_k != val:
                        self.counter += 1
                        self._add_output(val, min_k)
