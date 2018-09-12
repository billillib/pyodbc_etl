import pandas as pd
import pyodbc
import getpass
import yaml
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("yaml")
args = parser.parse_args()

# Read yaml config
with open(args.yaml, 'r') as f:
    config = yaml.load(f)

# Set connection properties
server = config["server"]["endpoint"]
database = config["server"]["database"]
username = config["server"]["username"]
password = getpass.getpass()
cnxn = pyodbc.connect('DRIVER=/usr/local/lib/libmsodbcsql.17.dylib;SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# Set destination properties
local_filename = config["file"]["local_filename"]
server_filename = config["file"]["server_filename"]
tablename = local_filename[str.rfind(local_filename, '/')+1:].replace('.', '_')  # to replace the filename -> file_name_csv

# Read file to get headers
df = pd.read_csv(local_filename)
rows = df.shape[0]  # to be inserted to a metadata col
cols = df.shape[1]
fields = df.columns

# Drop table first if it exists
sql = 'DROP TABLE IF EXISTS ' + tablename
cursor.execute(sql)
cursor.commit()

# Create destination table
sql = ''

for counter, field in enumerate(fields):
    last_col = len(fields) - 1
    if counter < last_col:
        sql = sql + ('['+field+']' + ' varchar(500),') + '\n'
    else:
        sql = sql + ('['+field+']' + ' varchar(500)')

sql_create = 'CREATE TABLE ' + tablename + '(' + '\n' + sql + ')'

cursor.execute(sql_create)
cursor.commit()

# Bulk insert into destination table
bulk_insert = 'BULK INSERT ' + tablename + ' FROM "' + server_filename + '" WITH (FIELDTERMINATOR = '"','"',ROWTERMINATOR = '"'0x0a'"');'
cursor.execute(bulk_insert)
cursor.commit()

# Close connection
cnxn.close()

