import os
import os.path
from os import path
from traceback import print_tb
import netCDF4 as nc4
import numpy as np
from progressbar import ProgressBar
import pandas as pd
import csv
from datetime import datetime, timedelta
from netCDF4 import num2date
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

######################## DATA ##########################################

path_cd = r'Data/Climate'
cd_files = sorted(os.listdir(path_cd))
end = len(cd_files)


coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.05.csv')
dict={}
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

point_lat = pd.read_csv(r'Dimensioncsv/medi_lat.csv')
point_lon = pd.read_csv(r'Dimensioncsv/medi_lon.csv')

############################# INDEX ###################################
points_lat = point_lat['lat'][:].to_numpy()
points_lon = point_lon['lon'][:].to_numpy()
# print(point_lat,point_lon)

strpro = stringClass()
################################ Date Processing ################################ 
def date_info(date):
    if (len(str(date))) <2:
        return '0' + str(date)
    else:
        return str(date)


points = []
for i in points_lat[:]:
    for j in points_lon[:]:
        points.append([i,j])

grid = 0.05
index_cd = []
for point in points[:]:
    latitude = point[0]
    longitude = point[1]
    ilat = int(abs(np.floor((-90-latitude)/grid)))
    ilon = int(abs((-180-longitude)/grid))
    index_cd.append([ilat,ilon])
#print(index_cd)
for i in range (end):
    k=[]
    df_cd = nc4.Dataset(path_cd+'\\'+cd_files[i])
    name = cd_files[i]
    # print(name)
    cd_actual = df_cd['sea_surface_temperature'][:,:]
    times = df_cd.variables['time'][:]
    units = df_cd.variables['time'].units  
    t = num2date(times, units = units, calendar='365_day') 
    t -= timedelta(days=9)
    c = str(t[0].year) + date_info(t[0].month) + date_info(t[0].day)
    if 'night' in str(name):
        d = c + 'N'
    elif 'day' in str(name):
        d = c + 'D'
                  
    for pos,p in zip(index_cd,points):
        c1 = int(p[0] * 100)
        k1 = c1 - c1%5
        c2 = int(p[1] * 100)
        k2 = c2 - c2%5
        data = cd_actual[0][pos[0],pos[1]].astype(float)
        # print(d,dict[k1],dict[k2],data)
        if cd_actual.mask[0][pos[0],pos[1]] == False:
            data = cd_actual[0][pos[0],pos[1]].astype(float)
            # print(d,dict[k1],dict[k2],data)
            cursor.execute('''DELETE FROM climate_temperature  \
                WHERE date= %s and latitude = %s and longitude = %s;''', (d,dict[k1],dict[k2]))
            cursor.execute('''INSERT INTO climate_temperature(date, latitude, longitude, sea_surface_temperature) \
                         VALUES (%s,%s, %s, %s);''', (d,dict[k1],dict[k2],data)) 
        else:
            data = None
            k.append(data)
    
    # with open(r'fact_tables/Climate_Temperature_null.csv', 'a', newline="", encoding='UTF8') as f:
    #         writer = csv.writer(f)
    #         writer.writerow([name,len(k)])
   

        
