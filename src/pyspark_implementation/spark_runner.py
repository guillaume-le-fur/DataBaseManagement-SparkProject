import pyspark
import pandas as pd
from pyspark.sql import functions as f
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


class SparkRunner:

    def __init__(self, data_file, sep='\\s+', row_limit=None, skip_rows=None):
        if data_file is not None:
            self.spark_context = pyspark.SparkContext()
            self.sql_context = pyspark.SQLContext(self.spark_context)
            self.initial_data = self.sql_context.createDataFrame(
                pd.read_csv(data_file, sep=sep, nrows=row_limit, skiprows=skip_rows), ['f', 't']
            )
            self.data = self.sql_context.createDataFrame(
                pd.read_csv(data_file, sep=sep, nrows=row_limit, skiprows=skip_rows), ['f', 't']
            )
            self.counter = 1
            self.old_data = self.sql_context.createDataFrame(pd.DataFrame([[0, 1]], columns=['f', 't']))
            self.final_output = None
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
            data = self.data
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
            self.data.show()
        data = self.map(data)
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
        """
        Runs the map-reduce algorithm until convergence

        :param data: The data to use. If not provided, the file provided when creating the instance will be used.
        :param verbose: Should the intermediate steps be displayed.
        :return: The output of the last map-reduce algorithm, the connected components of the graph.
        """
        reduce_out = None
        while self.counter > 0:
            reduce_out = self.reduce(data, verbose=verbose)
        self.final_output = reduce_out
        return reduce_out

    def save_output(self):
        if self.final_output:
            self.final_output.toPandas().to_csv('./out/spark_out/output.csv', index=False)

    def load_output(self):
        self.final_output = self.sql_context.createDataFrame(pd.read_csv('./out/spark_out/output.csv'))

    def plot_graph(self):
        color_palette = ['red', 'green', 'blue']
        pd_initial_data = self.initial_data.toPandas()
        pd_output_data = self.final_output.toPandas()
        graph = nx.Graph()
        from_nodes = pd_initial_data.loc[:, 'f'].unique()
        to_nodes = pd_initial_data.loc[:, 't'].unique()
        distinct_clusters = list(pd_output_data.loc[:, 't'].unique())
        number_clusters = len(distinct_clusters)
        colors = {}
        palette_index = 0
        for cluster in distinct_clusters:
            colors[cluster] = color_palette[palette_index]
            palette_index = (palette_index + 1) % len(color_palette)
        print(f'Color palette : {colors}')
        all_nodes = list(set(list(from_nodes) + list(to_nodes)))
        color_set = []
        for node in all_nodes:
            graph.add_node(node)
            if node in distinct_clusters:
                color_set.append(colors[node])
            else:
                print(f'Node {node}')
                row = pd_output_data[pd_output_data['f'] == node]
                print(row)
                # TODO debug nodes that don't have a cluster
                if row.shape[0]> 0:
                    cluster = row.values[0][1]
                    print(f'Node : {node}, cluster {cluster}')
                    color_set.append(colors[cluster])
                else:
                    color_set.append('black')

        for index, edge in pd_initial_data.iterrows():
            graph.add_edge(edge[0], edge[1])

        nx.draw(graph, node_color=color_set, with_labels=True)
        plt.show()

        # graph.add_nodes_from()
        # graph.


# r = SparkRunner('test-paper.txt')
# r.run(verbose=True).show()

# r = SparkRunner('test-graph.txt')
# r.load_output()
# r.final_output.show()
# # r.run(verbose=True).show()
# # r.save_output()
# r.plot_graph()

r2 = SparkRunner('web-Google.txt', row_limit=100, skip_rows=6) # TODO check exact value, not 6
r2.run()
r2.plot_graph()
