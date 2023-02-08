import pglast
import pglast.ast
from pglast.enums import ConstrType
from pglast.stream import maybe_double_quote_name
from .helpers import debugprint, eprint

class Column:
    def __init__(self):
        self.name = None
        self.has_default = False
        self.is_null = True
        self.typename = None
        self.charlen = -1
        self.charlen2 = -1
        
    def __str__(self):
        c1 = '' if self.charlen < 0 else '{}'.format(self.charlen)
        c2 = '' if self.charlen2 < 0 else ',{}'.format(self.charlen2)
        c1 = c1 + c2
        return '{} type:{}{} {} {}'.format(
            self.name,
            self.typename, 
            '' if len(c1) < 0 else '({})'.format(c1),
            '' if self.is_null else 'NOT NULL', 
            '' if not self.has_default else 'DEFAULT',
            )
    def __rep__(self):
        return self.__str__()
        
def safe_name(name):
    return maybe_double_quote_name(name)

class Table:
    def __init__(self):
        self.name = None
        self.schema = None
        self.columns = []
        # list of tuples [k1, k2] of unique keys
        self.unique_constraints = []
        # {'columns' : [k1,k2] , 'reftable' : tablename, 'refcolumns' : [c1, c2]}
        self.foreignkey_constraints = []
     
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

        if self.unique_constraints or self.foreignkey_constraints:
            l.append('')

        if self.unique_constraints:
            l.append('Unique Constraints')
            l.append('------------------')
            l.extend(list(map(str, self.unique_constraints)))
            l.append('')

        if self.foreignkey_constraints:
            l.append('Foreign Constraints')
            l.append('-------------------')
            for fk in self.foreignkey_constraints:
                s= '{} references {}({})'.format(fk['columns'], fk['reftable'], fk['refcolumns'])
                l.append(s)
            l.append('')
            
        return '\n'.join(l)
  
    def __rep__(self):
        return self.__str__()
        

def parse(sql):
    '''
    Parse the given sql and return a list of tables
    '''
    root = None
    try:
        root=pglast.parse_sql(sql)
    except pglast.parser.ParseError as e:
        ex_str=str(e)
        pos = ex_str.find('at index ')
        eprint('Error parsing schema : {}'.format(e))
        if pos >= 0:
            pos = int(ex_str[pos+9:])
            debugprint(pos, sql[pos-10:pos+10])
            lines = sql.count('\n', 0, pos)
            eprint('@ line : {}'.format(lines+1))
            startpos = sql.rfind('\n', 0, pos)
            endpos = sql.find('\n', pos)
            # sanitize
            if startpos < 0: 
                startpos = 0
            else:
                startpos += 1
            if endpos < 0: endpos=len (sql)
            if endpos - startpos > 80 : 
                startpos = max(0, pos-10)
                endpos = min(pos+10, len(sql))
            print('>>> {}'.format(sql[startpos:endpos]))
        return []

    tables = []
    for raw_stmt in root:
        st = raw_stmt.stmt

        if type(st) == pglast.ast.IndexStmt:
            if not st.unique:
                eprint('not a unique index, skipping over')
            else:
                cols = [key.name for key in st.indexParams]
                eprint('unique idx:', cols)

        elif type(st) == pglast.ast.AlterTableStmt:
            for cmd in [cmd for cmd in st.cmds if type(cmd) == pglast.ast.AlterTableCmd]:
                if cmd.def_.contype.name == 'CONSTR_PRIMARY':
                    cols = [k.val for k in cmd.def_.keys]
                    eprint('primary key:', cols)
                elif cmd.def_.contype.name == 'CONSTR_FOREIGN':
                    fk = [k.val for k in cmd.def_.fk_attrs]
                    pk = [k.val for k in cmd.def_.pk_attrs]
                    pktable = cmd.def_.pktable.relname
                    eprint('foreign key: {} on {}.{}'.format(fk,pktable,pk))
        elif type(st) == pglast.ast.CreateStmt:
            table = Table()
            table.name = st.relation.relname
            table.schema = st.relation.schemaname
    
            for col in st.tableElts:
                if type(col) == pglast.ast.Constraint:
                    # check for primary key
                    if col.contype.name == 'CONSTR_PRIMARY':
                        p_cols = [k.val for k in col.keys]
                        debugprint('primary keys:', p_cols)
                        table.unique_constraints.append(p_cols)
                    elif col.contype.name == 'CONSTR_FOREIGN':
                        fk = [k.val for k in col.fk_attrs]
                        pk = [k.val for k in col.pk_attrs]
                        pktable = col.pktable.relname
                        debugprint('foreign key: {} on {}.{}'.format(fk, pktable, pk))
                        # {'columns' : [k1,k2] , 'reftable' : tablename, 'refcolumns' : [c1, c2]}
                        table.foreignkey_constraints.append({
                            'columns' : fk,
                            'reftable' : pktable,
                            'refcolumns' : pk
                        })
                    elif col.contype.name == 'CONSTR_UNIQUE':
                        p_cols = [k.val for k in col.keys]
                        debugprint('primary keys:', p_cols)
                        table.unique_constraints.append(p_cols)

                elif type(col) == pglast.ast.ColumnDef:
                    column = Column()
                    column.name = col.colname

                    # type
                    for tn in col.typeName.names:
                        if tn.val == 'pg_catalog':
                            continue
                        column.typename = tn.val

                    length = -1
            
                    if col.typeName.typmods is not None and len(col.typeName.typmods) > 0 :
                        column.charlen = col.typeName.typmods[0].val.val
                        if len(col.typeName.typmods)>1:
                            column.charlen2 = col.typeName.typmods[1].val.val
            
                    if col.constraints is not None and len(col.constraints) > 0:
                        for c in col.constraints:
                            if c.contype == ConstrType.CONSTR_NOTNULL:
                                column.is_null = False
                            elif c.contype == ConstrType.CONSTR_DEFAULT:
                                column.has_default=True
        
                    #print(column)
                    table.columns.append(column)
                else:
                    debugprint('not processing :  {}'.format(col.__class__.__name__))
                    continue  
            tables.append(table)
            debugprint(table)
        else:
            debugprint('not processing : {}'.format(st.__class__.__name__))
            continue
    return tables