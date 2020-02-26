from src.consts import Consts
from src.deduplicator.dedup_grouper import DedupGrouper
from src.deduplicator.dedup_reducer import DedupReducer
from src.iterate.iterate_mapper import IterateMapper
from src.iterate.iterate_reducer import IterateReducer
from src.iterate.iterate_sorter import IterateSorter
from src.iterate.iterate_grouper import IterateGrouper
from src.deduplicator.dedup_mapper import DedupMapper
from src.data_generator import DataGenerator
import pandas as pd


class Runner:

    def __init__(self, input_file=None, separator='\\s+'):
        print(f'Input file {input_file} will be used, with separator : {separator}')
        self.input_file = input_file
        self.separator = separator

    def iterate_while_changes(self, verbose=False):
        iteration = 1
        # Init to one is arbitrary, doesn't have any semantic meaning
        counter = 1
        old_data = pd.DataFrame(columns=['from', 'to'])
        while counter > 0:
            print(f'### Iteration {iteration} ###')
            new_data = self.run(verbose=verbose)
            counter = abs(new_data.shape[0] - old_data.shape[0])
            if verbose:
                print(f'Number of changes : {counter}')
            old_data = new_data
            self.input_file = Consts.DEDUPL_REDUCER_OUTPUT_FILE
            self.separator = ','
            iteration += 1

    def run(self, verbose=False):
        im = IterateMapper(self.input_file, self.separator)

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
        print(dr)
        dr.out_output(index=False)

        return ir.output


file_name = Consts.GENERATED_DATA_DEFAULT_NAME
dg = DataGenerator()
dg.data_as_file(100, 100, file_name)
r = Runner(Consts.GENERATED_DATA_DEFAULT_NAME, ',')
r.iterate_while_changes()
