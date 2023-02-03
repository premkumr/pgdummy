import string
from faker.providers import BaseProvider
from .helpers import eprint
from . import helpers

class SequenceGenerator:
    def __init__(self, start=1, step=1):
        self.now =start
        self.step = step
        
    def next(self):
        n = self.now
        self.now += self.step
        return n

# create new provider class
class SimpleProvider(BaseProvider):
    def integer(self, max = 100000, min = 0) -> int:
        '''
        generate a random integer between min(0) and max(10000)
        '''
        return self.random_int(min, max)
        
    def decimal(self, max = 100000.0, min = 0, precision=3, maxdigits=None) -> float:
        '''
        generate a random decimal between min(0.0) and max(10000.0) with a precision(3) [eg 4.567]
        '''
        if maxdigits:
            num = self.random_number(digits=maxdigits)
        else:
            num = min + self.generator.random.random() * (max-min)
        if precision > 0:
            p = pow(10,precision)
            num = num/p
            
        return num
        
    def timestamp(self, start = '-30d', end='now', format='%Y-%m-%d %H:%M:%S') -> str:
        '''
        generate a random time between start(-30d) and end(now) of format('%Y-%m-%d %H:%M:%S')
        '''
        return helpers.fake.date_time_between(start, end).strftime(format)

    def string(self, max=16, min = 1, pattern=None, letters=string.ascii_uppercase):
        '''
        generate a random string of the given pattern(eg: '%Y-%m-%d %H:%M:%S')
        - Number signs ('#') are replaced with a random digit (0 to 9).
        - Question marks ('?') are replaced with a random character
        '''
        if pattern:
            return self.bothify(pattern, letters=letters)
        else:
            l = self.generator.random.randint(min, max)
            return ''.join(self.generator.random.choices(letters, k=l))
    
    def oneof(self, items=[0]):
        '''
        select one of the elements from the specified [items] list
        '''
        return self.random_element(items)
        
    def alphanumeric(self, max=16, min = 1):
        '''
        generate a random alphanumeic[A-Z0-9] string of length varying between min(1), max(16)
        '''
        l =   self.generator.random.randint(min, max)
        return ''.join(self.generator.random.choices(string.ascii_uppercase + string.digits, k=l))

    def hex(self, pattern='^^^^^^^'):
        '''
        generate a random hex string[A-Z0-9] on the given pattern
        '''
        return helpers.fake.hexify(pattern)

    def foreign(self, key):
        '''
        re-use the same set of values from the a different table.column
        '''
        return helpers.cache.get(key)

class DistinctGenerator:
    def __init__(self, fn, maxcount=20):
        self.fn = fn
        self.maxcount = maxcount
        self.seen = set()
        
    def next(self):

        if len(self.seen) < self.maxcount:
            item = self.fn()
            self.seen.add(item)
        else:
            item = helpers.fake.random_element(self.seen)
            
        return item

class UniqueException(Exception):
    pass

class UniqueGenerator:
    def __init__(self, fn):
        self.fn = fn
        self.seen = set()
        self.maxtries = 1000
        
    def next(self):
        for n in range(self.maxtries):
            item = self.fn()
            if item not in self.seen:
                self.seen.add(item)
                return item
        raise UniqueException ('could not find unique item within {} tries'.format(self.maxtries))    
        return None 

# Get the default generator mapping for pg datatypes
def get_default_generator(column):
    g = {}
    if column.typename in ['smallserial','serial','bigserial']:
        g['generator'] = 'sequence'
        g['start'] = 1
        g['step'] = 1

    elif column.typename in ['bigint','int8']:
        g['generator'] = 'integer'
        g['max'] = 1000000

    elif column.typename in ['int', 'int2', 'int4', 'money']:
        g['generator'] = 'integer'
        g['max'] = 1000

    elif column.typename in ['real','float4','float8','double precision']:
        g['generator'] = 'decimal'
        g['max'] = 10000
        g['precision'] = 4

    elif column.typename in ['numeric', 'decimal']:
        g['generator'] = 'decimal'
        g['max'] = 100
        if column.charlen > 0:
            g['maxdigits'] = column.charlen
        if column.charlen2 > 0:
            g['precision'] = column.charlen2

    elif column.typename in ['bpchar', 'varchar']:
        g['generator'] = 'string'
        l = 5 if column.charlen <= 0 else column.charlen
        g['max'] = l
        g['min'] = l

    elif column.typename in ['char', 'character', 'character varying']:
        g['generator'] = 'string'
        l = 1 if column.charlen <= 0 else column.charlen
        g['max'] = l
        g['min'] = l

    elif column.typename in ['timestamp','timestamptz']:
        g['generator'] = 'timestamp'
        g['start'] = '-30d'
        g['end'] = 'now'
        g['format'] = '%Y-%m-%d %H:%M:%S'

    elif column.typename in ['time']:
        g['generator'] = 'timestamp'
        g['start'] = '-1d'
        g['end'] = 'now'
        g['format'] = '%H:%M:%S'

    elif column.typename in ['date']:
        g['generator'] = 'timestamp'
        g['start'] = '-30d'
        g['end'] = 'now'
        g['format'] = '%Y-%m-%d'

    elif column.typename in ['bool']:
        g['generator'] = 'boolean'

    elif column.typename in ['uuid']:
        g['generator'] = 'uuid4'

    elif column.typename in ['text']:
        g['generator'] = 'string'
        g['max'] = 8
        g['min'] = 8

    elif column.typename in ['macaddr']:
        g['generator'] = 'hex'
        g['pattern'] = '^^:^^:^^:^^:^^:^^'

    elif column.typename in ['macaddr8']:
        g['generator'] = 'hex'
        g['pattern'] = '^^:^^:^^:^^:^^:^^:^^:^^'

    elif column.typename in ['inet']:
        g['generator'] = 'ipv6'

    elif column.typename in ['cidr']:
        g['generator'] = 'ipv6'
        g['network'] = True

    elif column.typename in ['uuid']:
        g['generator'] = 'uuid4'

    elif column.typename in ['bytea']:
        g['generator'] = 'hex'
        g['pattern'] = '\\' + 'x^^^^^'

    elif column.typename in ['bit']:
        g['generator'] = 'string'
        g['max'] = 8
        g['letters'] = '01'

    else:
        eprint(' -->>> UNKNOWN TYPE: {} '.format(column.typename))

    return g