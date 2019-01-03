import pyodbc
import getpass
import yaml
from subprocess import call
from collections import namedtuple, defaultdict


class Database:

    def __init__(self, server, database, azure=False, username=None, password=None, debug=None ):
        self.server = server
        self.database = database
        self.azure = azure
        self.username = username
        self.password = password
        self.conn = None
        self.debug = debug


    def __enter__(self):
        if self.azure == False:
            self.conn = pyodbc.connect('Driver={SQL Server};SERVER='+self.server+';DATABASE='+self.database+';Trusted_Connection=yes;')
            self.conn.autocommit = True
        elif self.azure == True:
            self.conn = pyodbc.connect('Driver={SQL Server};SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
            self.conn.autocommit = True

    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
            self.conn = None


def get_all_tables(database):
    '''
    Input: database object with context manager
    Output: List of all tables in db
    '''
    sql = 'select CONCAT(table_schema,\'.\',table_name) from INFORMATION_SCHEMA.TABLES'

    with database:
        all_tables = database.conn.cursor().execute(sql)
        all_tables = [table[0] for table in list(all_tables.fetchall())]

    return all_tables


def truncate_table(database,table):
    sql = 'TRUNCATE TABLE {}'.format(table)
    with database:
        database.conn.cursor().execute(sql)


def truncate_all_tables(database):

    tables = get_all_tables(database)

    for table in tables:
        truncate_table(database,table)


def truncate_all_tables(database):

    tables = get_all_tables(database)

    for table in tables:
        truncate_table(database,table)


def load_raw_file(database,table_schema,table_name,source_file):
    '''
    Input: database object, table schema and name, source_file
    Debug database connection will only load 1000 records
    Can assert the database object exists prior.
    '''
    if database.azure == False and database.debug == False:
        bcp = ('bcp {}.{}.{} IN "{}" -t "\\t" -r "0x0a" -C 65001 -S {} -T -F 2 -c -b 10000'
               .format(database.database,table_schema,table_name,source_file,database.server))

    elif database.azure == False and database.debug == True:
        bcp = ('bcp {}.{}.{} IN "{}" -t "\\t" -r "0x0a" -C 65001 -S {} -T -F 2 -c -b 10000 -L 1000'
               .format(database.database,table_schema,table_name,source_file,database.server))

    elif database.azure == True:
        bcp = ('bcp {}.{}.{} IN "{}" -t "\\t" -r "0x0a" -C 65001 -U {} -P {} -S {} -F 2 -c -a 65535 -b 10000'
               .format(database.database,table_schema,table_name,source_file,database.username,database.password, database.server))

    call(bcp, shell=True)


def get_tables_to_load(yaml_file):
    '''
    Input: YAML config file
    Output: Dictionary with tablename as key, value is namedtuple
    '''
    with open(yaml_file, 'r') as f:
        list_of_tables_to_load = yaml.load(f)

    TableRecord = namedtuple('TableRecord','table_schema, table_name, source_file')
    raw_tables_to_load = defaultdict(TableRecord)

    # create the dictionary
    for records in list_of_tables_to_load:
        for k,v in records.items():
            t = TableRecord._make(v)
        raw_tables_to_load[t.table_name] = t

    return raw_tables_to_load


def load_many_raw_files(database,tables):
    '''
    Input: dictionary of files to load (values = TableOject named tuple, database object)

    '''
    for v in tables.values():
        load_raw_file(database,v.table_schema,v.table_name,v.source_file)


def db_full_load(database):
    '''
    Executes the full_load sproc for the database
    '''
    sql = 'EXEC Metadata.usp_full_load'

    with database:
        database.conn.cursor().execute(sql)
