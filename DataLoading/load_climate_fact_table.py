from multiprocessing import Pool
import os
import netCDF4 as nc4
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from netCDF4 import num2date
from stringclass import stringClass
from sqlalchemy import create_engine, text
from time import time
import math

def date_info(date):
    if(len(str(date))) < 2:
        return '0' + str(date)
    else:
        return str(date)
    
def generate_keys(lat,lon):
    # Handle rounding differently for positive and negative values
    c1 = round(lat * 100)
    c1 = c1 - 1 if c1 < 0 else c1 
    k1 = c1 - (c1 % 5)  # calculate the closest multiple of 5

    c2 = round(lon * 100)
    c2 = c2 - 1 if c2 < 0 else c2 
    k2 = c2 - (c2 % 5)  # calculate the closest multiple of 5
    
    return k1, k2

def process_file(args):
    file, path_cd, index_cd, points, dict_lat_lon, connection_string = args
    df_cd = nc4.Dataset(os.path.join(path_cd, file))
    name = file
    cd_actual = df_cd['sea_surface_temperature'][:,:]
    times = df_cd.variables['time'][:]
    units = df_cd.variables['time'].units
    t = num2date(times, units=units, calendar='365_day')
    t -= timedelta(days=9)
    c = str(t[0].year) + date_info(t[0].month) + date_info(t[0].day)
    if 'night' in str(name):
        d = c + 'N'
    elif 'day' in str(name):
        d = c + 'D'

    data_list = []
    no_data_list =[]
    cli_point_list = []
    for pos, p in zip(index_cd, points):
        k1, k2 = generate_keys(p[0],p[1])
        cli_point_list.append([k1,k2])
        if not cd_actual.mask[0][pos[0], pos[1]]:
            data = cd_actual[0][pos[0], pos[1]].astype(float)
            data_list.append({'date': d, 'latitude': dict_lat_lon[k1], 'longitude': dict_lat_lon[k2],
                              'sea_surface_temperature': data})

    data_df = pd.DataFrame(data_list)
    engine = create_engine(connection_string)

    try:
        data_df.to_sql('climate_temperature', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("Error occurred during data insertion:", str(e))

def load_climate(connection_info):
    start_time = time()
    path_cd = r'Data/climate'
    cd_files = sorted(os.listdir(path_cd))

    coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.05.csv')
    dict_lat_lon = {}
    co_lon = coords_lon['degree005'][:].to_numpy()

    for c in co_lon:
        range_start = float(c.split('~')[0])  # Extract the range start value
    
        # Convert the range start value to an integer key
        lonl = int(range_start * 100)
        if (lonl%5 == 1):
            lonl = lonl - 1
        if (lonl%5 == 4):
            lonl = lonl + 1
        dict_lat_lon[lonl] = c
    # pri nt(dict_lat_lon)

    point_lat = pd.read_csv(r'Dimensioncsv/medi_lat.csv')
    point_lon = pd.read_csv(r'Dimensioncsv/medi_lon.csv')
    points_lat = point_lat['lat'][:20].to_numpy()
    points_lon = point_lon['lon'][:20].to_numpy()

    strpro = stringClass()

    grid_lat, grid_lon = np.meshgrid(points_lat, points_lon)

    # Flatten the grid to get 1D arrays of latitudes and longitudes
    points_lat_flat = grid_lat.flatten()
    points_lon_flat = grid_lon.flatten()

    # Create the points array from the flattened latitude and longitude arrays
    points = np.array([points_lat_flat, points_lon_flat]).T

    grid = 0.05

    ilats = np.abs(np.floor((-90 - points_lat_flat) / grid)).astype(int)
    ilons = np.abs(np.floor((-180 - points_lon_flat) / grid)).astype(int)

    index_cd = list(zip(ilats, ilons))

    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"

    with Pool() as pool:
        pool.map(process_file, [(file, path_cd, index_cd, points, dict_lat_lon, connection_string) for file in cd_files])

    end_time = time()
    
    return end_time - start_time

