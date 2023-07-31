import psycopg2

def fact_constraints(cursor):
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

    

