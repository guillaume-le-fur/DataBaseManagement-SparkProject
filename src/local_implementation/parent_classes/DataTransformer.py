import pandas as pd


class DataTransformer:

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.data = None
        self.output = None

    def load_data(self, header=0, sep=',', row_limit=None, skip_rows=None):
        self.data = pd.read_csv(self.input_file, header=header, sep=sep, nrows=row_limit, skiprows=skip_rows)

    def create_output(self):
        if self.data is not None and isinstance(self.data, pd.DataFrame):
            self.output = pd.DataFrame(columns=self.data.columns)

    def out_data(self, index=False):
        if self.data is not None:
            self.data.to_csv(self.output_file, index=index)

    def out_output(self, index=False):
        if self.output is not None:
            self.output.to_csv(self.output_file, index=index)

    def _add_output(self, f, t):
        if self.output is not None:
            self.output = self.output.append(
                pd.Series(
                    data=[f, t],
                    index=self.output.columns
                ),
                ignore_index=True
            )

    def __str__(self):
        if self.output is not None:
            return '--- ' + self.__class__.__name__ + ' output ---\n' + self.output.__str__()
