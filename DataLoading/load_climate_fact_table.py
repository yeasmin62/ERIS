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

def date_info(date):
    if(len(str(date))) < 2:
        return '0' + str(date)
    else:
        return str(date)

def process_file(args):
    file, path_cd, index_cd, points, dict, connection_string = args
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
    for pos, p in zip(index_cd, points):
        c1 = int(p[0] * 100)
        k1 = c1 - c1 % 5
        c2 = int(p[1] * 100)
        k2 = c2 - c2 % 5
        if not cd_actual.mask[0][pos[0], pos[1]]:
            data = cd_actual[0][pos[0], pos[1]].astype(float)
            data_list.append({'date': d, 'latitude': dict[k1], 'longitude': dict[k2],
                              'sea_surface_temperature': data})

    data_df = pd.DataFrame(data_list)
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text("CREATE TEMPORARY TABLE temp_table AS SELECT * FROM climate_temperature WITH NO DATA;"))
            data_df.to_sql('temp_table', con=connection, if_exists='append', index=False)
            connection.execute(text("""
                INSERT INTO climate_temperature (date, latitude, longitude, sea_surface_temperature)
                SELECT date, latitude, longitude, sea_surface_temperature FROM temp_table
                ON CONFLICT (date, latitude, longitude) DO NOTHING;
            """))
            connection.execute(text(f"DROP TABLE temp_table;"))  # Delete the temporary table

def load_climate(connection_info):
    start_time = time()
    path_cd = r'Data/climate'
    cd_files = sorted(os.listdir(path_cd))

    coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.05.csv')
    dict = {}
    co_lon = coords_lon['degree005'][:].to_numpy()
    for c in co_lon[:]:
        for i in c:
            if(i == '~'):
                lonl = int(float(c[:c.index(i) - 1]) * 100)
                if(lonl % 5 == 1):
                    lonl = lonl - 1
                if(lonl % 5 == 4):
                    lonl = lonl + 1
                dict[lonl] = c

    point_lat = pd.read_csv(r'Dimensioncsv/medi_lat.csv')
    point_lon = pd.read_csv(r'Dimensioncsv/medi_lon.csv')
    points_lat = point_lat['lat'][:].to_numpy()
    points_lon = point_lon['lon'][:].to_numpy()

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
        pool.map(process_file, [(file, path_cd, index_cd, points, dict, connection_string) for file in cd_files])

    end_time = time()

    return end_time - start_time