# from Months import Months
import psycopg2
import os
  # alwyas change database name before running if you want to insert data in a new database
with open('config.txt', 'r') as file:
    lines = file.readlines()

conn_params = {}
for line in lines:
    key, value = line.strip().split('=')
    conn_params[key] = value

# check before running in which database you want to insert

conn = psycopg2.connect(
    host=conn_params['host'],
    port=conn_params['port'],
    database=conn_params['database'],
    user=conn_params['user'],
    password=conn_params['password']
)
  
conn.autocommit = True
cursor = conn.cursor()

script_directory = os.path.dirname(os.path.abspath(__file__))
relative_file_path = 'Dimensioncsv/Dates.csv'
absolute_file_path = os.path.join(script_directory, relative_file_path)
relative_file_path1 = 'Dimensioncsv/Months.csv'
absolute_file_path1 = os.path.join(script_directory, relative_file_path)
relative_file_path2 = 'Dimensioncsv/Semi-Day.csv'
absolute_file_path2 = os.path.join(script_directory, relative_file_path)
relative_file_path3 = 'Dimensioncsv/Degree_0.20.csv'
absolute_file_path3 = os.path.join(script_directory, relative_file_path)



sql = ['''COPY Dates(id,week,
month, monthofyear,year)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Dates.csv')),'''COPY Months(id,monthofyear,
year)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Months.csv')),'''COPY Semiday(id,dateofsemiday,
time)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Semi-Day.csv')),'''COPY Resolution020(Degree020)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Degree_0.20.csv')),'''COPY Resolution005(Degree005,Degree020)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Degree_0.05.csv')),'''COPY Resolution004(Degree004, Degree020)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Degree_0.04.csv')),'''COPY Resolution001(Degree001,Degree004,Degree005, Degree020)
FROM '{}'
DELIMITER ','
CSV HEADER;'''.format(os.path.join(script_directory, 'Dimensioncsv/Degree_0.01.csv'))]

for query in sql:
  cursor.execute(query)

###########Registering into schema###########

schemaInsertion = ['''insert into schema (tablename, fieldname, key, varfree)
 values ('Dates', 'id', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Dates', 'week', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Dates', 'month', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Dates', 'monthofyear', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Dates', 'year', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Semiday', 'id', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Semiday', 'dateofsemiday', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Semiday', 'time', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution005', 'Degree005', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution005', 'Degree020', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution004', 'Degree004', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution004', 'Degree020', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution020', 'Degree020', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution001', 'Degree001', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution001', 'Degree004', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution001', 'Degree005', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Resolution001', 'Degree020', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Months', 'id', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Months', 'monthofyear', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Months', 'year', true, false);''']

for query in schemaInsertion:
  cursor.execute(query)

conn.close()

