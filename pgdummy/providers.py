import string
from faker import Faker
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
        return self.random_int(min, max)
        
    def decimal(self, max = 100000.0, min = 0, precision=3, maxdigits=None) -> float:
        if maxdigits:
            num = self.random_number(digits=maxdigits)
        else:
            num = min + self.generator.random.random() * (max-min)
        if precision > 0:
            p = pow(10,precision)
            num = num/p
            
        return num
        
    def timestamp(self, start = '-30d', end='now', format='%Y-%m-%d %H:%M:%S') -> str:
        return helpers.fake.date_time_between(start, end).strftime(format)

    def string(self, max=16, min = 1, pattern=None):
        if pattern:
            return self.bothify(pattern, letters=string.ascii_uppercase)
        else:
            l = self.generator.random.randint(min, max)
            return ''.join(self.generator.random.choices(string.ascii_uppercase, k=l))
    
    def oneof(self, items=[0]):
        return self.random_element(items)
        
    def alphanumeric(self, max=16, min = 1):
        l =   self.generator.random.randint(min, max)
        return ''.join(self.generator.random.choices(string.ascii_uppercase + string.digits, k=l))

    def foreign(self, key):
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
