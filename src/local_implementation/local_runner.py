from src.local_implementation.consts import Consts
from src.local_implementation.deduplicator.dedup_grouper import DedupGrouper
from src.local_implementation.deduplicator.dedup_reducer import DedupReducer
from src.local_implementation.iterate.iterate_mapper import IterateMapper
from src.local_implementation.iterate.iterate_reducer import IterateReducer
from src.local_implementation.iterate.iterate_sorter import IterateSorter
from src.local_implementation.iterate.iterate_grouper import IterateGrouper
from src.local_implementation.deduplicator.dedup_mapper import DedupMapper
from src.data_generator import DataGenerator
import pandas as pd
import time
import matplotlib.pyplot as plt


class LocalRunner:
    # TODO find a way to include a row limit in data.

    def __init__(self, input_file=None, separator='\\s+', row_limit=None, skip_rows=None):
        print(f'Input file {input_file} will be used, with separator : {separator}')
        self.input_file = input_file
        self.separator = separator
        self.row_limit = row_limit
        self.skip_rows = skip_rows
        self.timer = 0.

    def run(self, verbose=False):
        initial_time = time.time()
        iteration = 0
        # Init to one is arbitrary, doesn't have any semantic meaning
        counter = 1
        old_data = pd.DataFrame(columns=['from', 'to'])
        while counter > 0:
            if verbose:
                print(f'### Iteration {iteration} ###')
            new_data = self.iterate(verbose=verbose, iteration=iteration)
            counter = abs(new_data.shape[0] - old_data.shape[0])
            if verbose:
                print(f'Number of changes : {counter}')
            old_data = new_data
            self.input_file = Consts.DEDUPL_REDUCER_OUTPUT_FILE
            self.separator = ','
            iteration += 1
        self.timer = time.time() - initial_time

    def iterate(self, verbose=False, iteration=0):
        im = IterateMapper(
            self.input_file,
            self.separator,
            iteration=iteration,
            row_limit=self.row_limit,
            skip_rows=self.skip_rows
        )

        if verbose:
            print('--- Initial data ---')
            print(im.data)
        im.map()
        if verbose:
            print(im)
        im.out_output(index=False)

        i_sort = IterateSorter()
        i_sort.sort()
        if verbose:
            print(i_sort)
        i_sort.out_output()

        ig = IterateGrouper()
        ig.group()
        if verbose:
            print(ig)
        ig.out_output(index=True)

        ir = IterateReducer()
        ir.reduce()
        if verbose:
            print(ir)
        ir.out_output(index=False)

        dm = DedupMapper()
        dm.map()
        if verbose:
            print(dm)
        dm.out_output(index=False)

        dg = DedupGrouper()
        dg.group()
        if verbose:
            print(dg)
        dg.out_output(index=False)

        dr = DedupReducer()
        dr.reduce()
        if verbose:
            print(dr)
        dr.out_output(index=False)

        return ir.output


# file_name = Consts.GENERATED_DATA_DEFAULT_NAME
# dg = DataGenerator()
# n_list = [10, 100, 1000, 10000]
# time_list = []
# for n in n_list:
#     dg.data_as_file(n, 100, file_name)
#     r = LocalRunner(Consts.GENERATED_DATA_DEFAULT_NAME, ',')
#     r.run()
#     time_list.append(r.timer)
#
# plt.plot(n_list, time_list)
# plt.show()
