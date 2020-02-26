from src.consts import Consts
from src.parent_classes.DataTransformer import DataTransformer


class DedupReducer(DataTransformer):

    def __init__(self):
        super().__init__(Consts.DEDUPL_GROUP_OUTPUT_FILE, Consts.DEDUPL_REDUCER_OUTPUT_FILE)
        self.load_data()
        self.create_output()

    def reduce(self):
        for index, row in self.data.iterrows():
            f, t = row['from'].split(',')
            self._add_output(f, t)

