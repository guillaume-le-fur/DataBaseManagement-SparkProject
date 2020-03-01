from src.local_implementation.consts import Consts
from src.local_implementation.parent_classes.DataTransformer import DataTransformer


class IterateSorter(DataTransformer):

    def __init__(self):
        super().__init__(Consts.ITERATE_MAP_OUTPUT_FILE, Consts.ITERATE_SORT_OUTPUT_FILE)
        self.load_data()
        self.create_output()

    def sort(self):
        self.output = self.data.sort_values(by=['from'])
