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

########################### Primary Keys #################
primaryKeys = ['''ALTER TABLE copernicus_temperature ADD CONSTRAINT copernicus_temperature_pk PRIMARY KEY ("date",latitude,longitude);''',
'''ALTER TABLE climate_temperature ADD CONSTRAINT climate_temperature_pk PRIMARY KEY ("date",latitude,longitude);''',
'''ALTER TABLE modisaqua_temperature ADD CONSTRAINT modisaqua_temperature_pk PRIMARY KEY ("date",latitude,longitude);''',
'''ALTER TABLE pathfinder_temperature ADD CONSTRAINT pathfinder_temperature_pk PRIMARY KEY ("date",latitude,longitude);''']

for q in primaryKeys:
    cursor.execute(q)


########################### Foreign Keys #################
foreignKeys = ['''ALTER TABLE copernicus_temperature ADD CONSTRAINT copernicus_temperature_fk3 FOREIGN KEY (latitude) 
REFERENCES resolution005(degree005) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE copernicus_temperature ADD CONSTRAINT copernicus_temperature_fk2 FOREIGN KEY (longitude) 
REFERENCES resolution005(degree005) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE copernicus_temperature ADD CONSTRAINT copernicus_temperature_fk FOREIGN KEY ("date") 
REFERENCES dates(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE climate_temperature ADD CONSTRAINT climate_temperature_fk FOREIGN KEY ("date") 
REFERENCES Semiday(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE climate_temperature ADD CONSTRAINT climate_temperature_fk2 FOREIGN KEY (latitude) 
REFERENCES Resolution005(degree005) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE climate_temperature ADD CONSTRAINT climate_temperature_fk3 FOREIGN KEY (longitude) 
REFERENCES Resolution005(degree005) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE modisaqua_temperature ADD CONSTRAINT modisaqua_temperature_fk FOREIGN KEY ("date") 
REFERENCES dates(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE modisaqua_temperature ADD CONSTRAINT modisaqua_temperature_fk2 FOREIGN KEY (latitude) 
REFERENCES Resolution004(degree004) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE modisaqua_temperature ADD CONSTRAINT modisaqua_temperature_fk3 FOREIGN KEY (longitude) 
REFERENCES Resolution004(degree004) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE pathfinder_temperature ADD CONSTRAINT pathfinder_temperature_fk FOREIGN KEY ("date") 
REFERENCES SEMIDAY(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE pathfinder_temperature ADD CONSTRAINT pathfinder_temperature_fk2 FOREIGN KEY (latitude) 
REFERENCES Resolution004(degree004) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''',
'''ALTER TABLE pathfinder_temperature ADD CONSTRAINT pathfinder_temperature_fk3 FOREIGN KEY (longitude) 
REFERENCES Resolution004(degree004) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE;''']

for q in foreignKeys:
    cursor.execute(q)

schemaInsertion = ['''insert into schema (tablename, fieldname, key, varfree)
 values ('Copernicus_Temperature', 'date', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Copernicus_Temperature', 'latitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Copernicus_Temperature', 'longitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Copernicus_Temperature', 'Sea_surface_temperature', false, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Climate_Temperature', 'date', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Climate_Temperature', 'latitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Climate_Temperature', 'longitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Climate_Temperature', 'Sea_surface_temperature', false, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Modisaqua_Temperature', 'date', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Modisaqua_Temperature', 'latitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Modisaqua_Temperature', 'longitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Modisaqua_Temperature', 'Sea_surface_temperature', false, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Pathfinder_Temperature', 'date', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Pathfinder_Temperature', 'latitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Pathfinder_Temperature', 'longitude', true, false);''',
 '''insert into schema (tablename, fieldname, key, varfree)
 values ('Pathfinder_Temperature', 'Sea_surface_temperature', false, false);''']

for query in schemaInsertion:
  cursor.execute(query)

conn.close()