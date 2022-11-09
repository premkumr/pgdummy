

class Writer:
    def __init__(self):
        pass

    def table(self, tablename, column_infos):
        pass

    def row(self, columns):
        pass

    def table_end(self, tablename):
        pass


class InsertWriter(Writer):
    def __init__(self):
        self.tablename = None
        self.sqlt = []
    
    def table(self, tablename, column_names):
        self.sqlt = []
        self.sqlt.append('INSERT INTO {} ('.format(tablename))
        self.sqlt.append(','.join(column_names))
        self.sqlt.append(') VALUES({});')

        self.sqlt = ' '.join(self.sqlt)
        self.tablename = tablename
        print('--')
        print('-- data for [{}]'.format(tablename))
        print('--')

    def row(self, columns):
        values = []
        for v in columns:
            if type(v) == str:
                values.append("'{}'".format(v))
            else:
                values.append(str(v))
        print(self.sqlt.format(','.join(values)))

    def table_end(self, tablename):
        print()
        print()


class DumpWriter(Writer):
    def __init__(self):
        self.tablename = None
        self.once = False

    def printHeader(self):
        if self.once : return
        self.once = True
        print("""
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

        """)
    
    def table(self, tablename, column_names):
        self.printHeader()
        self.sqlt = []
        self.sqlt.append('COPY {} ('.format(tablename))
        self.sqlt.append(','.join(column_names))
        self.sqlt.append(') FROM stdin;')

        self.sqlt = ' '.join(self.sqlt)
        self.tablename = tablename
        print('-- ')
        print('-- data for [{}]'.format(tablename))
        print('-- ')
        print()
        print(self.sqlt)

    def row(self, columns):
        print('\t'.join(map(str , columns)))

    def table_end(self, tablename):
        print('\.')
        print()
