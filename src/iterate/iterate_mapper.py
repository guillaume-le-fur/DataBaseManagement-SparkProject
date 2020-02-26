#!/home/guillaume/Documents/python-virtualenvs/spark-env/bin/python

from src.consts import Consts
from src.parent_classes.DataTransformer import DataTransformer


class IterateMapper(DataTransformer):

    def __init__(self, input_file=None, separator='\\s+'):
        if input_file is None:
            input_file = Consts.ITERATE_MAP_INPUT_FILE
        output_file = Consts.ITERATE_MAP_OUTPUT_FILE
        super().__init__(input_file, output_file)
        self.load_data(header=0, sep=separator)
        self.create_output()

    def map(self):
        for index, row in self.data.iterrows():
            self._add_output(row['from'], row['to'])
            self._add_output(row['to'], row['from'])
