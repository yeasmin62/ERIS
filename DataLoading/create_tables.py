import psycopg2

def table_creation(conn, cursor):
  sql = ['''DROP TABLE IF EXISTS schema CASCADE;
  CREATE TABLE IF NOT EXISTS public.schema
  (
      tablename text COLLATE pg_catalog."default" NOT NULL,
      fieldname text COLLATE pg_catalog."default" NOT NULL,
      key boolean NOT NULL,
      varfree boolean DEFAULT false,
      CONSTRAINT schema_pkey PRIMARY KEY (tablename, fieldname)
  )
  TABLESPACE pg_default;''','''DROP TABLE IF EXISTS Dates CASCADE;
  create table Dates (
    id char(8) not null,
    week char(7) not null,
    month char(6) not null,
    monthOfYear char(2) not null,
    year char(4) not null);
  DELETE FROM schema WHERE tablename='Dates';''' , '''DROP TABLE IF EXISTS SemiDay CASCADE;
  create table SemiDay (
    id char(9) not null,
    dateofsemiday char(8) not null,
    time char(1) not null);
  DELETE FROM schema WHERE tablename='SemiDay';''', '''DROP TABLE IF EXISTS Resolution005 CASCADE;
  create table Resolution005 (
    Degree005 char(17) not null,
    Degree020 char(17) not null);
  DELETE FROM schema WHERE tablename='Resolution005';''','''DROP TABLE IF EXISTS Months CASCADE;
  create table Months (
    id char(6) not null,
    monthofyear char(2) not null,
    year char(4) not null);
  DELETE FROM schema WHERE tablename='Month';''', '''DROP TABLE IF EXISTS Resolution004 CASCADE;
  create table Resolution004 (
    Degree004 char(17) not null,
    Degree020 char(17) not null);
  DELETE FROM schema WHERE tablename='Resolution004';''','''DROP TABLE IF EXISTS Resolution020 CASCADE;
  create table Resolution020 (
    Degree020 char(17) not null);
  DELETE FROM schema WHERE tablename='Resolution020';''','''DROP TABLE IF EXISTS Resolution001 CASCADE;
  create table Resolution001 (
    Degree001 char(17) not null,
    Degree004 char(17) not null,
    Degree005 char(17) not null,
    Degree020 char(17) not null);
  DELETE FROM schema WHERE tablename='Resolution001';''','''DROP TABLE IF EXISTS Copernicus_Temperature CASCADE;
  CREATE TABLE Copernicus_Temperature (
    date char(9) NOT NULL,
    latitude char(17) NOT NULL,
    longitude char(17) NOT NULL,
    sea_surface_temperature FLOAT);
  DELETE FROM schema WHERE tablename='Copernicus_Temperature';''','''DROP TABLE IF EXISTS Climate_Temperature CASCADE;
  CREATE TABLE Climate_Temperature (
    date char(9) NOT NULL,
    latitude char(17) NOT NULL,
    longitude char(17) NOT NULL,
    sea_surface_temperature FLOAT);
  DELETE FROM schema WHERE tablename='Climate_Temperature';''','''DROP TABLE IF EXISTS PathFinder_Temperature CASCADE;
  CREATE TABLE PathFinder_Temperature (
    date char(9) NOT NULL,
    latitude char(17) NOT NULL,
    longitude char(17) NOT NULL,
    sea_surface_temperature FLOAT);
  DELETE FROM schema WHERE tablename='PathFinder_Temperature';''','''DROP TABLE IF EXISTS ModisAqua_Temperature CASCADE;
  CREATE TABLE ModisAqua_Temperature (
    date char(9) NOT NULL,
    latitude char(17) NOT NULL,
    longitude char(17) NOT NULL,
    sea_surface_temperature FLOAT);
    DELETE FROM schema WHERE tablename='ModisAqua_Temperature';''']


  for query in sql:
    cursor.execute(query)
  

