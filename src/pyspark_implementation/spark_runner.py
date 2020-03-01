import pyspark
import pandas as pd
from pyspark.sql import functions as f
import numpy as np


class SparkRunner:

    def __init__(self, data_file, sep='\\s+'):
        if data_file is not None:
            self.spark_context = pyspark.SparkContext()
            self.sql_context = pyspark.SQLContext(self.spark_context)
            self.data = self.sql_context.createDataFrame(pd.read_csv(data_file, sep=sep), ['f', 't'])
            self.counter = 1
            self.old_data = self.sql_context.createDataFrame(pd.DataFrame([[0, 1]], columns=['f', 't']))
        else:
            raise ValueError("File path shouldn't be None.")

    def map(self, data=None):
        if data is None:
            data = self.data
        return data.rdd.flatMap(lambda tup: [(tup[0], tup[1]), (tup[1], tup[0])]).toDF(['f', 't'])

    def reduce(self, data=None, verbose=False):
        if verbose:
            print('--- INPUT DATA ---')
            self.data.show()
        if data is None:
            data = self.map()
        # TODO See if counter has to be used this way.
        self.counter = np.abs(self.old_data.count() - data.count())
        self.old_data = data
        grouped_data = data.groupBy('f') \
            .agg(f.min('t').alias('min_t'))
        first_map_reduce_output = data.join(
            grouped_data,
            'f',
            'inner'
        )
        reduce_out = first_map_reduce_output \
            .withColumn(
                'max_f_t',
                f.greatest(
                    first_map_reduce_output.f,
                    first_map_reduce_output.t
                )
            ) \
            .selectExpr('max_f_t as f', 'min_t as t') \
            .filter('f > t') \
            .dropDuplicates() \
            .groupBy('f')\
            .agg(f.min('t').alias('t'))\
            .sort(['f', 't'])
        self.data = reduce_out

        if verbose:
            print('--- REDUCE OUTPUT ---')
            reduce_out.show()
            print(f'Value of the counter : {self.counter}')
        return reduce_out

    def run(self, data=None, verbose=False):
        reduce_out = None
        while self.counter > 0:
            reduce_out = self.reduce(data, verbose=verbose)
        return reduce_out


# r = SparkRunner('test-paper.txt')
# r.run(verbose=True).show()

r = SparkRunner('test-graph.txt')
r.run(verbose=True).show()

