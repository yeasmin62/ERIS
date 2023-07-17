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

def process_file(args):

    filename, dict, connection_string, time_chunk = args
    nc = nc4.Dataset(filename, 'r', Format='NETCDF4')  # Open the dataset in the process
    lats = nc.variables['lat'][:]
    lons = nc.variables['lon'][:]
    times = nc.variables['time'][time_chunk]
    units = nc.variables['time'].units
    data_list = []
    batch_size = 10000  # Adjust batch size as per your requirement
    pid = os.getpid()  # Get current process ID
    temp_table_name = f"temp_table_{pid}"  # Create a unique temp table name for this process

    for time_index, time in enumerate(times):
        t = num2date(time, units=units, calendar='365_day')
        t -= timedelta(days=9)
        c = str(t.year) + date_info(t.month) + date_info(t.day)
        sfc = nc.variables['analysed_sst'][time_index, :, :]  # Load data for one time step at a time

        for lat_index, lat in enumerate(lats):
            c1 = int(lat * 100)
            k1 = c1 - c1%5
            for lon_index, lon in enumerate(lons):
                c2 = int(lon * 100)
                k2 = c2 - c2%5
                data = sfc[lat_index, lon_index].astype(float)

                if sfc.mask[lat_index, lon_index] == False:
                    data_list.append({'date': c, 'latitude': dict[k1], 'longitude': dict[k2],
                              'sea_surface_temperature': data})

        data_df = pd.DataFrame(data_list)
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            with connection.begin():
                data_df.to_sql(temp_table_name, con=connection, if_exists='append', index=False)
                connection.execute(text(f"""
                    INSERT INTO copernicus_temperature (date, latitude, longitude, sea_surface_temperature)
                    SELECT date, latitude, longitude, sea_surface_temperature FROM {temp_table_name}
                    ON CONFLICT (date, latitude, longitude) DO NOTHING;
                """))
                connection.execute(text(f"DROP TABLE {temp_table_name};"))  # Delete the temporary table

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
        for i in c:
            if(i=='~'):
                lonl = int(float(c[:c.index(i)-1])*100)
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