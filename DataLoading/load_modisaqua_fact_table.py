import os
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

def load_modsaqua(conn, cursor):
######################## DATA ##########################################

    path_cd = r'Data\modisaqua'
    cd_files = sorted(os.listdir(path_cd))
    end = len(cd_files)


    coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.04.csv')
    co_lon = coords_lon['Degree004'][:].to_numpy()
    dict={}
    for c in co_lon[:]:
        for i in c:
            if(i=='~'):
                lonl = int(float(c[:c.index(i)-1])*100)
                # print(lonl)
                if(lonl%4==1):
                    lonl = lonl - 1
                    # print(lonl) 
                if (lonl%4==3):
                    lonl = lonl + 1
                dict[lonl]=c
    # print(dict)         
    point_lat = pd.read_csv(r'Dimensioncsv/medi_lat.csv')
    point_lon = pd.read_csv(r'Dimensioncsv/medi_lon.csv')

    points_lat = point_lat['lat'][:].to_numpy()
    points_lon = point_lon['lon'][:].to_numpy()
    ############################# INDEX ###################################

    strpro = stringClass()
    ################################ Date Processing ################################ 
    def date_info(date):
        if (len(str(date))) <2:
            return '0' + str(date)
        else:
            return str(date)

    ################################ Combination of latlon ################################ 


    points = []
    for i in points_lat[:]:
        for j in points_lon[:]:
            points.append([i,j])

    ################################ Point Index ################################ 
    grid = 0.04166
    index_cd = []
    for point in points[:]:
        latitude = point[0]
        longitude = point[1]
        ilat = int(abs(np.floor((90-latitude)/grid)))
        ilon = int(abs((-180-longitude)/grid))
        index_cd.append([ilat,ilon])

    #print(index_cd,points)
    date = datetime(2020,1,1)
    for i in range (end):
        k=[]
        df_cd = nc4.Dataset(path_cd+'\\'+cd_files[i])
        name = cd_files[i]
        cd_actual = df_cd['sst4'][:]
        d = str(date.year) + date_info(date.month) + date_info(date.day)
            
        for pos,p in zip(index_cd,points):
            c1 = int(p[0] * 100)
            k1 = c1 - c1%4
            c2 = int(p[1] * 100)
            k2 = c2 - c2%4

            if cd_actual.mask[pos[0],pos[1]] == False:
                data = cd_actual[pos[0],pos[1]].astype(float)
                # print(d,dict[k1],dict[k2],data)
                cursor.execute('''DELETE FROM modisaqua_temperature  \
                    WHERE date= %s and latitude = %s and longitude = %s;''', (d,dict[k1],dict[k2]))
                cursor.execute('''INSERT INTO modisaqua_temperature(date, latitude, longitude, sea_surface_temperature) \
                            VALUES (%s,%s, %s, %s);''', (d,dict[k1],dict[k2],data)) 
            else:
                data = None
                k.append(data)
        
        # with open(r'fact_tables/modisaqua_chlorophyll_null.csv', 'a', newline="", encoding='UTF8') as f:
        #         writer = csv.writer(f)
        #         writer.writerow([name,len(k)])
            
                
        date += timedelta(1)
    
        
