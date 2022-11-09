class SuperDict(dict):
    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        
    def __getattr__(self, attr):
        return self.__dict__.get(attr, None)

    def __setattr__(self, attr, value):
        self.__dict__[attr] = value

    def __getitem__(self, key):
        return self.__dict__.get(key, None)
        
    def __str__(self):
        return str(self.__dict__)
     
    def get_all(self):
        return self.__dict__
        
    def __contains__(self, name):
        return name in self.__dict__
        
    def __iter__(self):
        return self.__dict__.__iter__()
        
    def __next__(self):
        return self.__dict__.__next__()

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)


class Column(SuperDict):
    def __init__(self):
        super(SuperDict,self).__init__()
        self.name = None
        self.has_default = False
        self.is_null = True
        self.type = None
        self.charlen = None
        self.charlen2 = None
        
    def __str__(self):
        c1 = '{}'.format(self.charlen) if self.charlen else ''
        c2 = ',{}'.format(self.charlen2) if self.charlen2 else ''
        c1 = c1 + c2
        return '{} type:{}{} {} {}'.format(
            self.name,
            self.type, 
            '' if len(c1) < 0 else '({})'.format(c1),
            '' if self.is_null else 'NOT NULL', 
            '' if not self.has_default else 'DEFAULT',
            )
    def __repr__(self):
        return self.__str__()
        
class Table(SuperDict):
    def __init__(self):
        super(SuperDict,self).__init__()
        self.name = None
        self.schema = None
        self.columns : list[Column] = []
     
    def get_name(self):
        return '{}{}'.format(
            '' if self.schema is None else '{}.'.format(self.schema),
            self.name
        )

    def __str__(self):
        l = []
        l.append('Table: {}'.format(self.get_name()))
        l.append('Columns')
        l.append('----------')
        for c in self.columns:
            l.append(str(c))
            
        return '\n'.join(l)
  
    def __repr__(self):
        return self.__str__()