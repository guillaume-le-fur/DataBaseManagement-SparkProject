import pandas as pd
import numpy as np
from src.consts import Consts


class DataGenerator:

    def __init__(self):
        pass

    def get_data(self, n, max_nodes=100):
        return pd.DataFrame(
            {
                'from': np.random.uniform(0, max_nodes, n),
                'to': np.random.uniform(0, max_nodes, n)
            }
        )

    def data_as_file(self, n, max_nodes=100, file_name=Consts.GENERATED_DATA_DEFAULT_NAME):
        self.get_data(n, max_nodes).to_csv(file_name, index=False)
