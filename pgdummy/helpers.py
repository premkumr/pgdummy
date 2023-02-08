import sys
from faker import Faker

COL_MAP_KEY_FMT = '{}.{}'
debug = False
fake = Faker()

def debugprint(*args, **kwargs):
    if debug:
        print(*args, file=sys.stderr, **kwargs)

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def set_seed(seed):
    Faker.seed(seed)

class Cache:
    def __init__(self):
        self.data = {}

    def add(self, tablename, columnname, value):
        key = '{}.{}'.format(tablename, columnname)
        items = self.data.setdefault(key, set())
        items.add(value)

    def get(self, key):
        items = self.data.setdefault(key, set())
        if len(items) == 0:
            return None
        return fake.random_element(items)

cache = Cache()


