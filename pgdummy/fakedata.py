#!/usr/bin/env python3
import argparse
import os.path
import random
import string
import sys
import inspect
from pathlib import Path
from typing import Optional

from . import helpers
from .config import Config
from .helpers import debugprint, eprint
from .providers import UniqueException
from .sqlparser import parse
from .writers import DumpWriter, InsertWriter, Writer


class Unique_Cache:
    def __init__(self):
        self.cache = {}

    def add(self, table, cols):
        key = '{}:{}'.format(table, str(cols.keys()))
        value = str(cols.values())
        self.cache.setdefault(key, set())
        if value in self.cache[key]:
            return False
        else:
            self.cache[key].add(value)
            return True

class DataGenerator:
    def __init__(self, config : Config):
        self.config = config
        self.unique_cache = Unique_Cache()
        self.table = None

    def rand_str(self, maxsize, minsize=1):
        l =   random.randint(minsize, maxsize)
        return ''.join(random.choices(string.ascii_uppercase, k=l))

    def get_value_for(self, colname, tablename):
        gen = self.config.get_generator(tablename, colname)
        return gen()

    def col(self, colname, tablename):
        return self.get_value_for(colname, tablename)

    def row(self, columns, tablename):
        if self.table is None or self.table['name'] != tablename:
            self.table = None
            for table in self.config.data['tables']:
                if table['name'] == tablename:
                    self.table = table
                    break

            if self.table is None:
                raise Exception('table [{}] - not found'.format(tablename))

        success = False
        attempt = 0
        max_attempts = 1000
        while not success and attempt < max_attempts:
            success = True
            attempt += 1
            foreigns = []
            valuemap={}
            for colname in columns:
                value = self.col(colname, tablename)
                valuemap[colname] = value
                colcfg = self.config.get_column(tablename,  colname)

                # store
                if 'is_foreignkey' in colcfg and colcfg['is_foreignkey']:
                    foreigns.append((colname, value))

            # unique constraints check
            colvalues = {}
            for unique in self.table['unique']:
                colvalues = {}
                for col in unique:
                    colvalues[col] = valuemap[col]

                if not self.unique_cache.add(tablename, colvalues):
                    debugprint ('Failed to unique : {}'.format(colvalues))
                    success = False
                    break

        if not success:
            raise Exception('unable to generate unique row : {}'.format(colvalues))

        # store only after valid row
        # store for foreign key lookup
        colvalues = []
        for colname in columns:
            colvalues.append(valuemap[colname])

        for fk in foreigns:
            helpers.cache.add(tablename, fk[0], fk[1])
            
        return colvalues

class DummyDB:
    def __init__(self):
        self.tables = []
        self.schema = ''
        self.config = Config()
        self.datagen = DataGenerator(self.config)
        self.seed = None

    def load_schema(self, filename):
        with open(filename) as f:
            self.schema = f.read()

        self.parse_schema(self.schema)

    def generate(self, schemafile, numrows=10):
        self.load_schema(schemafile)
        self.parse_schema(self.schema)
        self.generate_data(numrows)

    def parse_schema(self, sql):
        self.tables = parse(sql)
        for table in self.tables:
            self.config.add_table(table)
        return self.tables
        
    def generate_table_data(self, table, numrows, writer : Writer):
        self.config.validate()
        if self.seed:
            helpers.set_seed(self.seed)
        columns = [c.name for c in table.columns]
        writer.table(table.get_name(), columns)
    
        table_config = self.config.get_table(table.name)
        if 'numrows' in table_config:
            numrows = int(table_config['numrows'])

        failures = 0
        for n in range(numrows):
            values = []
            try:
                row = self.datagen.row(columns, table.name)
                writer.row(row)
            except UniqueException as e:
                failures += 1
                if failures > 10:
                    eprint('Unique failure exceeding limit .. stoppping {}'.format(table.name))
                    break
            
        writer.table_end(table.name)

    def generate_data(self, numrows=10, writer = DumpWriter(), tablefilter=[]):
        self.config.validate()
        order = self.config.get_safe_order()
        debugprint('table filter:', tablefilter)
        debugprint('topo sort : ', order)
        if len(order) != len(self.tables):
            eprint(order)
            eprint('something wrong.. topo sort messed up. {}!={}'.format(len(order) , len(self.tables)))

        # print order
        for n in order:
            table = self.tables[n]
            if len(tablefilter) > 0:
                if table.name not in tablefilter and table.get_name() not in tablefilter:
                    debugprint('skipping {} .. because of filter'.format(table.get_name()))
                    continue
                eprint('topo order:', n, table.name)

        for n in order:
            table = self.tables[n]
            _writer = writer
            if len(tablefilter) > 0:
                if table.name not in tablefilter and table.get_name() not in tablefilter:
                    debugprint('skipping {} .. because of filter'.format(table.get_name()))
                    # Empty writer, we do this for foreign key storage..
                    _writer= Writer()
            self.generate_table_data(table, numrows, _writer)
            
def cli_execute(argv: Optional[str] = None):
    argv = argv or sys.argv[:]
    prog_name = Path(argv[0]).name
    parser = argparse.ArgumentParser(prog=prog_name, description='Generate dummy data from sql schema')
    parser.add_argument('--no-summary', dest='summary', default = True, action='store_false')
    parser.add_argument('-s', '--schema', dest='schema', type=str, default=None, help = 'schema file to load')
    parser.add_argument('-c', '--config', dest='config', type=str, default=None, help = 'config file to use')
    parser.add_argument('-g', '--generate-config', default = False, action='store_true', help = 'generate config file')
    parser.add_argument('--help-gen', dest='help_gen', type=str, default=None, help = 'print help for generator')
    parser.add_argument('-n', '--numrows', dest='numrows', type=int, default=5, help = 'num rows to generate')
    parser.add_argument('--seed', dest='seed', type=int, default=None, help = 'value to seed the randomness')
    parser.add_argument('-v', '--verbose', default = False, action='store_true')
    parser.add_argument('-f', '--format', dest='format', choices=['insert', 'dump'], default='dump', nargs='?', help = 'output format')
    parser.add_argument('-t', '--table', dest='tables', action='append', help = 'process only these tables')
    
    args = parser.parse_args()

    dummy = DummyDB()
    dummy.seed = args.seed

    if args.help_gen:
        f = dummy.config.fake
        gen = None
        if args.help_gen in dir(f):
            gen = getattr(f, args.help_gen)
        if gen:
            options = inspect.signature(gen).parameters.values()
            if len(options) > 0:
                print('Options :: >')
                for option in options:
                    default_value = ''
                    tname = option.annotation.__name__
                    if tname.endswith('empty'):
                        tname = ''
                    if option.default is not None:
                        default_value = ' = {}'.format(option.default)
                    print('>> {} : {}{}'.format(option.name, tname, default_value))
                print()
            print(inspect.getdoc(gen))
            sys.exit(0)
        else:
            eprint('Generator : [{}] - not found'.format(args.help_gen))
            sys.exit(1)

    if not (args.schema or args.config):
        eprint('need to specify either schema or config file')
        sys.exit(1)

    if args.verbose:
        helpers.debug = True

    if args.schema:
        if not os.path.exists(args.schema):
            eprint('unable to locate schema : {}'.format(args.schema))
        else:
            dummy.load_schema(args.schema)

    if args.config:
        if args.generate_config and not os.path.exists(args.config):
            pass
        else:
            dummy.config.load(args.config)

    if args.generate_config:
        dummy.config.store(args.config)

    if args.numrows > 0:
        if args.generate_config:
            eprint('skipping row generation during conf generation ...')
        else:
            writer = None
            if args.format == 'insert':
                writer = InsertWriter()
            elif args.format == 'dump':
                writer = DumpWriter()

            tablefilter = args.tables if args.tables else []
            dummy.generate_data(numrows = args.numrows, writer = writer, tablefilter = tablefilter)

if __name__ == '__main__':
    cli_execute()
