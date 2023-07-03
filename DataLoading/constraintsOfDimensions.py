import psycopg2

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

########################### Primary Keys ################
primaryKeys = ['''ALTER TABLE dates ADD CONSTRAINT dates_pk PRIMARY KEY (id);''',
'''ALTER TABLE semiday ADD CONSTRAINT semiday_pk PRIMARY KEY (id);''',
'''ALTER TABLE resolution004 ADD CONSTRAINT resolution004_pk PRIMARY KEY (Degree004);''',
'''ALTER TABLE resolution005 ADD CONSTRAINT resolution005_pk PRIMARY KEY (Degree005);''',
'''ALTER TABLE resolution001 ADD CONSTRAINT resolution001_pk PRIMARY KEY (Degree001);''',
'''ALTER TABLE resolution020 ADD CONSTRAINT resolution020_pk PRIMARY KEY (Degree020);''']
for q in primaryKeys:
    cursor.execute(q)

########################### Foreign Keys #################
foreignKeys = ['''ALTER TABLE semiday ADD CONSTRAINT semiday_fk FOREIGN KEY (dateofsemiday)
 REFERENCES dates(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
 '''ALTER TABLE resolution001 ADD CONSTRAINT resolution001_fk FOREIGN KEY (degree004)
 REFERENCES resolution004(degree004) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
 '''ALTER TABLE resolution001 ADD CONSTRAINT resolution001_fk2 FOREIGN KEY (degree005)
 REFERENCES resolution005(degree005) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
 '''ALTER TABLE resolution001 ADD CONSTRAINT resolution001_fk3 FOREIGN KEY (degree020)
 REFERENCES resolution020(degree020) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
 '''ALTER TABLE resolution004 ADD CONSTRAINT resolution004_fk FOREIGN KEY (degree020)
 REFERENCES resolution020(degree020) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
 '''ALTER TABLE resolution005 ADD CONSTRAINT resolution005_fk FOREIGN KEY (degree020)
 REFERENCES resolution020(degree020) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''']

for q in foreignKeys:
    cursor.execute(q)

conn.close()