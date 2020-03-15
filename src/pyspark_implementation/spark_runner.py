import time

import pyspark
import pandas as pd
from pyspark.sql import functions as f
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


class SparkDataManager:

    def __init__(self, spark_context, initial_data_path, sep='\\s+', row_limit=None, skip_rows=None):
        if initial_data_path is not None:
            self.sql_context = pyspark.SQLContext(spark_context)
            self.initial_data = self.sql_context.createDataFrame(
                pd.read_csv(
                    initial_data_path,
                    sep=sep,
                    nrows=row_limit,
                    skiprows=skip_rows
                ),
                ['f', 't']
            )
            self.old_data = self.sql_context.createDataFrame(pd.DataFrame([[0, 1]], columns=['f', 't']))
            self.data = self.initial_data
            self.final_output = None
        else:
            raise ValueError("File path shouldn't be None.")

    def get_row_diff_and_update(self, new_data):
        diff = np.abs(self.old_data.count() - new_data.count())
        self.old_data = new_data
        return diff

    def show_data(self, full=False):
        if full:
            self.data.show(self.data.count(), False)
        else:
            self.data.show()

    def save_input(self, file_name):
        self.initial_data.toPandas().to_csv(f'out/spark_out/{file_name}', index=False)

    def save_output(self, file_name=None):
        if file_name is None:
            file_name = 'output.csv'
        if self.final_output:
            self.final_output.toPandas().to_csv(f'./out/spark_out/{file_name}', index=False)

    def load_output(self):
        self.final_output = self.sql_context.createDataFrame(pd.read_csv('./out/spark_out/output.csv'))


class SparkRunner:

    def __init__(self, spark_context, data_path, sep='\\s+', row_limit=None, skip_rows=None):
        self.data_path = data_path
        if data_path is not None:
            self.spark_context = spark_context
            self.sdm = SparkDataManager(
                spark_context=spark_context,
                initial_data_path=data_path,
                sep=sep,
                row_limit=row_limit,
                skip_rows=skip_rows
            )
            self.counter = 1
            self.timer = 0.
        else:
            raise ValueError("File path shouldn't be None.")

    def map(self, data=None):
        """
        For every edge A -> B, emits A -> B and B -> A

        :param data: the data to use for the map. self.data is used by default.
        I don't know if the parameter is useful, maybe for testing purposes
        :return: The mapped data, flattened to be pretty and reconverted to DF to keep the same structure.
        """
        if data is None:
            data = self.sdm.data
        # By doing a flatMap, we manage to emit both (from, to) and (to, from)
        # The output of the map function here is then just the edged duplicated in both ways.
        return data.rdd.flatMap(lambda tup: [(tup[0], tup[1]), (tup[1], tup[0])]).toDF(['f', 't'])

    def reduce(self, data=None, verbose=False):
        """
        # TODO Should this be broken down into functions ?
        This is a bit more than just a reduce.
        Steps performed in this function :
            - Calls the MAP.
            - Computes, for each 'from' node, the minimum 'to' node.
            - Joins the grouped data with the map output.
            - Computes the largest value between 'from' and 'min_t'
            - Filters the edges A -> B with A > B
            - Drops the duplicates.
            - Emits the corresponding edge.
            - Re-aggregates the data to find the minimum 'to' for each 'from'
            - Emits the result.

        :param data: The input data, by default, the output of self.map will be considered.
        :param verbose: Should the intermediate steps be displayed.
        :return: The output of the reduce steps.
        """
        if verbose:
            print('--- INPUT DATA ---')
            self.sdm.show_data()
        data = self.map(data)
        grouped_data = data.groupBy('f') \
            .agg(f.min('t').alias('min_t'))
        first_map_reduce_output = data.join(
            grouped_data,
            'f',
            'inner'
        )
        if verbose:
            print('--- MAP OUTPUT ---')
            first_map_reduce_output.show(first_map_reduce_output.count(), False)
        # TODO See if counter has to be used this way.
        self.counter = self.sdm.get_row_diff_and_update(first_map_reduce_output)

        reduce_out = grouped_data\
            .selectExpr('f as f', 'min_t as t').\
            union(
                first_map_reduce_output.filter('f > min_t').selectExpr('t as f', 'min_t as t')
            )\
            .sort(['f', 't']) \
            .dropDuplicates(['f', 't'])\
            .filter('f > t')

        self.sdm.data = reduce_out

        if verbose:
            print('--- REDUCE OUTPUT ---')
            # reduce_out.show(reduce_out.count(), False)
            self.sdm.show_data()
            print(f'Value of the counter : {self.counter}')
        return reduce_out

    def run(self, data=None, verbose=False):
        """
        Runs the map-reduce algorithm until convergence

        :param data: The data to use. If not provided, the file provided when creating the instance will be used.
        :param verbose: Should the intermediate steps be displayed.
        :return: The output of the last map-reduce algorithm, the connected components of the graph.
        """
        reduce_out = None
        start_time = time.time()
        while self.counter > 0:
            reduce_out = self.reduce(data, verbose=verbose)
        self.sdm.final_output = reduce_out
        self.timer = time.time() - start_time
        return reduce_out

    def plot_graph(self):
        # Color palette used for graph
        color_palette = ['red', 'green', 'blue', 'orange', 'purple', 'pink']
        # Convert data to pandas for convenience.
        pd_initial_data = self.sdm.initial_data.toPandas()
        pd_output_data = self.sdm.final_output.toPandas()
        # Getting nodes list
        from_nodes = pd_initial_data.loc[:, 'f'].unique()
        to_nodes = pd_initial_data.loc[:, 't'].unique()
        all_nodes = list(set(list(from_nodes) + list(to_nodes)))
        # Getting cluster for coloring
        distinct_clusters = list(pd_output_data.loc[:, 't'].unique())
        # Create a dictionary with structure {cluster : color}
        colors = {}
        palette_index = 0
        for cluster in distinct_clusters:
            colors[cluster] = color_palette[palette_index]
            # Using modulo to have infinite color list.
            palette_index = (palette_index + 1) % len(color_palette)
        # Instantiate graph
        graph = nx.Graph()
        color_set = []
        for node in all_nodes:
            # Adding the node to the graph
            graph.add_node(node)
            # Adding the color
            if node in distinct_clusters:
                # If the node is a cluster than assign it's color
                color_set.append(colors[node])
            else:
                # Else find the color of the cluster associated with the node.
                row = pd_output_data[pd_output_data['f'] == node]
                # This if is in case we don't find a color for the node.
                if row.shape[0] > 0:
                    cluster = row.values[0][1]
                    color_set.append(colors[cluster])
                else:
                    color_set.append('grey')

        # Append the edges.
        for index, edge in pd_initial_data.iterrows():
            graph.add_edge(edge[0], edge[1])

        nx.draw(graph, node_color=color_set, with_labels=True)
        plt.show()


# # r = SparkRunner('test-paper.txt')
# # r.run(verbose=True).show()
#
# # r = SparkRunner('test-graph.txt')
# # r.load_output()
# # r.final_output.show()
# # # r.run(verbose=True).show()
# # # r.save_output()
# # r.plot_graph()
# sc = pyspark.SparkContext()
# n_list = [10, 100, 1000, 10000]
# run_list = []
# for n in n_list:
#     r2 = SparkRunner(sc, 'web-Google.txt', row_limit=n, skip_rows=4)
#     r2.run()
#     run_list.append(r2.timer)
#     # print(r2.timer)
#     # r2.sdm.save_input('in_google.csv')
#     # r2.sdm.save_output('out_google.csv')
#     # r2.plot_graph()
#
# # One of the results.
# # run_list_4 = [23.50942587852478, 24.821873426437378, 39.00197744369507, 129.31728410720825]
#
# print(run_list)
# plt.plot(n_list, run_list)
# plt.show()
