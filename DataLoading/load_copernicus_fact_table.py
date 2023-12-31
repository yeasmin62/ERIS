from multiprocessing import Pool
import os
import netCDF4 as nc4
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from netCDF4 import num2date, Dataset
from stringclass import stringClass
from sqlalchemy import create_engine, text
from time import time

def date_info(date):
    if (len(str(date))) <2:
        return '0' + str(date)
    else:
        return str(date)
    
def generate_key_lat(lat):
    # Handle rounding differently for positive and negative values
    c1 = round(lat * 100)
    c1 = c1 - 1 if c1 < 0 else c1 + 1
    k1 = c1 - (c1 % 5)  # calculate the closest multiple of 5
    return k1

def generate_key_lon(lon):
    c2 = round(lon * 100)
    c2 = c2 - 1 if c2 < 0 else c2 + 1
    k2 = c2 - (c2 % 5)  # calculate the closest multiple of 5
    return k2

def process_file(args):

    filename, dict, connection_string, time_chunk = args
    nc = nc4.Dataset(filename, 'r', Format='NETCDF4')  # Open the dataset in the process
    lats = nc.variables['lat'][0:5]
    lons = nc.variables['lon'][0:5]
    times = nc.variables['time'][time_chunk]
    units = nc.variables['time'].units
    
    
    for time_index, time in enumerate(times):
        data_list = []
        t = num2date(time, units=units, calendar='365_day')
        t -= timedelta(days=9)
        c = str(t.year) + date_info(t.month) + date_info(t.day)
        sfc = nc.variables['analysed_sst'][time_index, :, :]  # Load data for one time step at a time

        for lat_index, lat in enumerate(lats):
            k1 = generate_key_lat(lat)
            for lon_index, lon in enumerate(lons):
                k2 = generate_key_lon(lon)
                

                if sfc.mask[lat_index, lon_index] == False:
                    data = sfc[lat_index, lon_index].astype(float)
                    data_list.append({'date': c, 'latitude': dict[k1], 'longitude': dict[k2],
                              'sea_surface_temperature': data})
                # else:
                #     data = None
                #     data_list.append({'date': c, 'latitude': dict[k1], 'longitude': dict[k2],
                #               'sea_surface_temperature': data})

        data_df = pd.DataFrame(data_list)
        engine = create_engine(connection_string)

        try:
            data_df.to_sql('copernicus_temperature', con=engine, if_exists='append', index=False)
        except Exception as e:
            print("Error occurred during data insertion:", str(e))

        engine.dispose()  # Dispose the engine after use

    nc.close()  # Close the dataset in the process


def load_copernicus(connection_info):
    start_time = time()
    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"

    dict = {}
    filename ='Data/copernicus/2020.nc'
    coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.05.csv')
    co_lon = coords_lon['degree005'][:].to_numpy()
    for c in co_lon[:]:
        range_start = float(c.split('~')[0])  # Extract the range start value
        # Convert the range start value to an integer key
        lonl = int(range_start * 100)
        if(lonl%5==1):
            lonl = lonl - 1
        if (lonl%5==4):
            lonl=lonl+1
        dict[lonl]=c

    nc = Dataset(filename, 'r', Format='NETCDF4')
    time_chunks = np.array_split(np.arange(len(nc.variables['time'][:3])), os.cpu_count())
    nc.close()

    with Pool() as pool:
        pool.map(process_file, [(filename, dict, connection_string, time_chunk) for time_chunk in time_chunks])

    end_time = time()

    return end_time - start_time

