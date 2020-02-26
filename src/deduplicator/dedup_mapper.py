from src.consts import Consts
from src.parent_classes.DataTransformer import DataTransformer


class DedupMapper(DataTransformer):

    def __init__(self):
        super().__init__(Consts.DEDUPL_MAP_INPUT_FILE, Consts.DEDUPL_MAP_OUTPUT_FILE)
        self.load_data()
        self.create_output()

    def map(self):
        for index, row in self.data.iterrows():
            self._add_output(str(row['from']) + ',' + str(row['to']), None)
