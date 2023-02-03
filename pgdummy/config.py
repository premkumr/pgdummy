import inspect
import json
import os.path
from functools import partial

import yaml
from faker import Faker

from . import helpers
from .helpers import debugprint, eprint
from .providers import (DistinctGenerator, SequenceGenerator, SimpleProvider,
                        UniqueGenerator, get_default_generator)


class Config:
    SYS_KEYS = ['type', 'name', 'has_default', 'is_null']
    STD_ARGS = ['min', 'max' , 'maxdigits', 'format','start', 'end', 'precision']
    def __init__(self):
        self.filename = None
        self.data = {"tables": []}
        self.genmap = {}
        self.colmap = {}
        self.fake = helpers.fake
        self.fake.add_provider(SimpleProvider)
        
    def __add_to_genmap(self, tablename, coldata):
        key = helpers.COL_MAP_KEY_FMT.format(tablename, coldata['name'])
        
        if not coldata or 'generator' not in coldata or len(coldata['generator']) == 0:
            eprint ('no valid info for {}: {}. -- {}'.format(tablename, coldata['name'], coldata))
            return False
    
        if not hasattr(self.fake, coldata['generator']) and coldata['generator'] != 'sequence':
            eprint ('no valid generator found {}: {}. -- {}'.format(tablename, coldata['name'], coldata))
            return False
    
        # fill args
        args = {}

        if coldata['generator'] != 'sequence':
            fn = getattr(self.fake, coldata['generator'])
        else:
            fn = SequenceGenerator
        
        for k in inspect.signature(fn).parameters.keys():
            if k in coldata:
                args[k] = coldata[k]

        if coldata['generator'] == 'sequence':
            fn = SequenceGenerator(**args).next
        else:
            # check for Distinct
            fn=partial(fn, **args)
            if 'distinct' in coldata:
                fn = DistinctGenerator(fn, coldata['distinct']).next
            if 'unique' in coldata:
                fn = UniqueGenerator(fn).next
            
        self.genmap[key] = fn
        
    def get_generator(self, tablename, colname):
        key = helpers.COL_MAP_KEY_FMT.format(tablename, colname)
        return self.genmap.get(key, None)
        
    def validate(self, force=False):
        if force:
            self.genmap={}

        foreigns = []
        for table in self.data["tables"]:
            for column in table['columns']:
                success = True
                if column['generator'] == 'foreign':
                    if 'key' not in column:
                        eprint('key not specified for foreign ref : {}.{}'.format(table['name'],column['name']))
                        raise Exception('foreign key not specified')
                    else:
                        foreigns.append(column['key'])
                if success and not self.get_generator(table['name'], column['name']):
                    self.__add_to_genmap(table['name'], column)

        # mark foreign key dependencies
        for key in foreigns:
            tn,cn = key.split('.')
            column = self.get_column(tn, cn)
            if not column : raise Exception('invalid foreign key spec [{}]'.format(key))
            column['is_foreignkey'] = True

        # check for circular dependencies ..
        for table in self.data["tables"]:
            for column in table['columns']:
                key = helpers.COL_MAP_KEY_FMT.format(table['name'],column['name'])
                colset = set()
                col = column
                while col['generator'] == 'foreign' and 'key' in col:
                    key = col['key']
                    t,_ = key.split('.')
                    if t in colset:
                        eprint ('circular foreign keys detected .. {}'.format(colset))
                        raise Exception('circular foreign keys')
                    colset.add(t)
                    col = self.colmap.get(col['key'], {})
                    
    def get_column(self, tablename, columnname):
        key = helpers.COL_MAP_KEY_FMT.format(tablename, columnname)
        return self.colmap.get(key, None)

    def get_table(self, tablename):
        for table in self.data['tables']:
            if table['name'] == tablename:
                return table

        return None

    def load(self, filename):
        if not os.path.exists(filename):
            eprint('config file NOT FOUND : {}'.format(filename))
            return
        with open(filename, "r") as fp:
            data = yaml.load(fp, Loader=yaml.FullLoader)
            self.__update_config(data)
            self.filename= filename

    def store(self, filename=None, minimal=True):
        # change the structure
        data = {'tables': {}}
        for table in self.data['tables']:
            t = {}
            for column in table['columns']:
                col={}
                for k in column.keys():
                    if k not in self.STD_ARGS + self.SYS_KEYS:
                        col[k] = column[k]
                t[column['name']] = col
            data['tables'][table['name']] = t

        indent = 4
        out = yaml.dump(data, indent=indent, sort_keys=False ,default_flow_style=False)
        #out = out.replace('\n ', '\n\n ')

        if filename is None:
            print(out) 
        else:
            with open(filename, "w") as fp:
                fp.write(out)

    def __update_config(self, newdata):
        if 'tables' not in newdata:
            return False

        for table in self.data['tables']:
            # find table in the conf
            if table['name'] in newdata['tables']:
                newtable = newdata['tables'][table['name']]
                if '__numrows' in newtable:
                    table['numrows'] = newtable['__numrows']

                for column in table['columns']:
                    # find table in conf
                    if column['name'] in newtable:
                        newcolumn = newtable[column['name']]
                        for k,v in newcolumn.items():
                            if k not in self.SYS_KEYS:
                                column[k] = v
                if '__unique' in newtable:
                    for unique in newtable['__unique']:
                        debugprint(unique)
                        if type(unique) == list:
                            table['unique'].append(unique)
                        else:
                            table['unique'].append([a.strip() for a in unique.split(',')])

        # setup the generators ..
        debugprint('final config')
        debugprint(json.dumps(self.data, indent=4))
        self.validate(force=True)

    def get_safe_order(self):
        graph = {}
        idxmap = {}
        count = 0
        for table in self.data["tables"]:
            idxmap[table['name']] = count
            count += 1
            graph[table['name']] = []
            edges = graph[table['name']]
            for column in table['columns']:
                if column['generator'] == 'foreign':
                    t, _ = column['key'].split('.')
                    if t not in edges:
                        edges.append(t)

        debugprint('graph',graph)
        seen = set()
        stack = []    # path variable is gone, stack and order are new
        order = []    # order will be in reverse order at first
        for k in graph.keys():
            if k not in seen:
                q = [k]
                while q:
                    #eprint(q, stack, order)
                    v = q.pop()
                    if v not in seen:
                        seen.add(v) # no need to append to path any more
                        q.extend(graph[v])

                        while stack and v not in graph[stack[-1]]: # new stuff here!
                            order.append(stack.pop())
                        stack.append(v)
                #eprint(q, stack, order)
                
        order = stack + order[::-1]   # new value!
        order.reverse()
        return [idxmap[name] for name in order]

    def add_table(self, table):
        # find existing table config
        t = next((item for item in self.data['tables'] if item["name"] == table.name), False)
        if not t:
            t = {
                'name' : table.name,
                'schema' : table.schema,
                'columns' : [],
                'unique' : []
            }
            self.data['tables'].append(t)
            
        for column in table.columns:
            c =  next((item for item in t['columns'] if item["name"] == column.name), False)

            if not c:
                c = {
                    'name' : column.name,
                    'type' : column.typename,
                    'is_null' : column.is_null,
                    'has_default' : column.has_default
                }
                t['columns'].append(c)
                key = helpers.COL_MAP_KEY_FMT.format(table.name, column.name)
                self.colmap[key] = c
            # check if generator has been set
            if 'generator' not in c or len(c['generator']) == 0:
                c.update(get_default_generator(column))
            # add to generator map
            #self.__add_to_genmap(table.name, c)

        t['unique'] = table.unique_constraints

