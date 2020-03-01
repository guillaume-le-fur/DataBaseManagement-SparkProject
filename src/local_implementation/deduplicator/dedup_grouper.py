from src.local_implementation.consts import Consts

from src.local_implementation.parent_classes.DataTransformer import DataTransformer


class DedupGrouper(DataTransformer):

    def __init__(self):
        super().__init__(Consts.DEDUPL_MAP_OUTPUT_FILE, Consts.DEDUPL_GROUP_OUTPUT_FILE)
        self.load_data()
        self.create_output()

    def group(self):
        self.output = self.data.drop_duplicates(subset=['from'])
