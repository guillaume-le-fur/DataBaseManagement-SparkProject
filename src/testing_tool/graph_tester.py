from src.local_implementation.local_runner import LocalRunner
from src.pyspark_implementation.spark_runner import SparkRunner
from src.local_implementation.consts import Consts
from src.data_generator import DataGenerator
import matplotlib.pyplot as plt
import pyspark
import pandas as pd


class GraphTester:

    def __init__(self):
        pass

    def test_local(self, input_file=None, separator='\\s+', row_limit=None, skip_rows=None):
        lr = LocalRunner(
            input_file=input_file,
            separator=separator,
            row_limit=row_limit,
            skip_rows=skip_rows
        )
        lr.run()
        print(lr.timer)
        return lr.timer

    def test_spark(self, spark_context, input_file=None, separator='\\s+', row_limit=None, skip_rows=None):
        sr = SparkRunner(
            spark_context=spark_context,
            data_path=input_file,
            sep=separator,
            row_limit=row_limit,
            skip_rows=skip_rows
        )
        sr.run()
        print(sr.timer)
        return sr.timer

    def test_with_generated_data(self, size_list=None, max_nodes=100):
        spark_context = pyspark.SparkContext()
        local_timers = []
        spark_timers = []
        dg = DataGenerator()
        for n in size_list:
            dg.data_as_file(n, max_nodes)
            local_timers.append(self.test_local(
                Consts.GENERATED_DATA_DEFAULT_NAME,
                ','
            ))
            spark_timers.append(self.test_spark(
                spark_context,
                Consts.GENERATED_DATA_DEFAULT_NAME,
                ','
            ))
        resulting_data = pd.DataFrame({
            'n': size_list,
            'local': local_timers,
            'spark': spark_timers
        })
        resulting_data.to_csv('./out/test_out/generated_out.csv')
        plt.plot(size_list, spark_timers, 'red')
        plt.plot(size_list, local_timers, 'blue')
        plt.show()

    def test_with_data_file(self, data_file, size_list=None):
        spark_context = pyspark.SparkContext()
        local_timers = []
        spark_timers = []
        for n in size_list:
            local_timers.append(self.test_local(
                data_file,
                row_limit=n,
                skip_rows=4
            ))
            spark_timers.append(self.test_spark(
                spark_context,
                data_file,
                row_limit=n,
                skip_rows=4
            ))
        resulting_data = pd.DataFrame({
            'n': size_list,
            'local': local_timers,
            'spark': spark_timers
        })
        resulting_data.to_csv('./out/test_out/data_out.csv')
        plt.plot(size_list, spark_timers, 'red')
        plt.plot(size_list, local_timers, 'blue')
        plt.show()


# gt = GraphTester()
# gt.test_with_generated_data(size_list=[10, 100, 1000])


gt = GraphTester()
gt.test_with_generated_data(size_list=[10, 100, 1000])
