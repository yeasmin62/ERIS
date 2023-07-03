from asyncio.windows_events import NULL
from math import ceil, floor
from pickle import LONG, NONE
from netCDF4 import Dataset, num2date, date2num
import csv
import pandas as pd
from datetime import datetime, timedelta
from stringclass import stringClass
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
  

cursor = conn.cursor()
conn.autocommit = True
dict = {}
filename ='Data/Copernicus/2020.nc'
coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.05.csv')
co_lon = coords_lon['Degree005'][:].to_numpy()
for c in co_lon[:]:
    for i in c:
        if(i=='~'):
            lonl = int(float(c[:c.index(i)-1])*100)
            # print(lonl)
            if(lonl%5==1):
                lonl = lonl - 1
            if (lonl%5==4):
                lonl=lonl+1
            dict[lonl]=c

# print(dict.keys())
nc = Dataset(filename, 'r', Format='NETCDF4')
ncv = nc.variables.keys()
lats = nc.variables['lat'][:]
# print(lats)
lons = nc.variables['lon'][:]
     
sfc= nc.variables['analysed_sst'][:]
# print(sfc)
times = nc.variables['time'][:3]
units = nc.variables['time'].units
#print(units)
# dates = num2date (times[:], units=units, calendar='365_day')
# print(dates[1].year, dates[1].month, dates[1].day)

strpro = stringClass()

# ################################ Date Processing ################################ 
def date_info(date):
    if (len(str(date))) <2:
        return '0' + str(date)
    else:
        return str(date)


# ################################ Pulling Out Data ################################    

k = []
for time_index, time in enumerate(times): # for a date
        #print(time_index,time)
    t = num2date(time, units = units, calendar='365_day')
    t -= timedelta(days=9)
    c = str(t.year) + date_info(t.month) + date_info(t.day)
    # pull out the data
    for lat_index,lat in enumerate(lats):
        c1 = int(lat * 100)
        k1 = c1 - c1%5
        for lon_index, lon in enumerate(lons):
            c2 = int(lon * 100)
            k2 = c2 - c2%5
            data = sfc[time_index, lat_index, lon_index].astype(float)
            # print(c,dict[k1],dict[k2],data)
            if sfc.mask[time_index, lat_index, lon_index]==False:
                data = sfc[time_index, lat_index, lon_index].astype(float)
                cursor.execute('''DELETE FROM copernicus_temperature  \
                WHERE date= %s and latitude = %s and longitude = %s;''', (c,dict[k1],dict[k2]))
                cursor.execute('''INSERT INTO copernicus_temperature(date, latitude, longitude, sea_surface_temperature) \
                         VALUES (%s,%s, %s, %s);''', (c,dict[k1],dict[k2],data)) 
                
            else:
                data = None
                k.append(data)
            #     # print(t,dict[k1],dict[k2],data)
                
                                
    # with open(r'fact_tables/copernicus_temperature_null.csv', 'a', newline="", encoding='UTF8') as f:
    #         writer = csv.writer(f)
    #         writer.writerow([c,len(k)]) 
nc.close()
