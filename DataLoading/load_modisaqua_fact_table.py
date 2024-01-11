import os
from multiprocessing import Pool
import netCDF4 as nc4
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from time import time
import constraints_of_facts
import analyze_table
import psycopg2

def generate_keys(lat,lon):
    # Handle rounding differently for positive and negative values
    c1 = round(lat * 100)
    c1 = c1 - 1 if c1 < 0 else c1 
    k1 = c1 - (c1 % 4)  # calculate the closest multiple of 5

    c2 = round(lon * 100)
    c2 = c2 - 1 if c2 < 0 else c2 
    k2 = c2 - (c2 % 4)  # calculate the closest multiple of 5
    
    return k1, k2

def process_file(args):
    filename, path_cd, index_cd, points, dict, connection_string = args
    file_split = filename.split('.')
    d = file_split[1]  # extract date from filename
    nc = nc4.Dataset(os.path.join(path_cd, filename))
    # print(nc.variables)
    if 'SST' in file_split:
        date = d + 'D'
        sst4 = nc['sst'][:]
    elif 'SST4' in file_split:
        date = d + 'N'
        sst4 = nc['sst4'][:]
    data_list = []
    for pos,p in zip(index_cd,points):
        k1,k2 = generate_keys(p[0],p[1])
        if not sst4.mask[pos[0],pos[1]]:
            data = sst4[pos[0],pos[1]].astype(float)
            data_list.append({'date': date, 'latitude': dict[k1], 'longitude': dict[k2], 'sea_surface_temperature': data})

    # Convert list of dictionaries to DataFrame
    data_df = pd.DataFrame(data_list)
    engine = create_engine(connection_string)
    data_df.to_sql('modisaqua_temperature', con=engine, if_exists='append', index=False)
    nc.close()


def load_modisaqua(connection_info):
    start_time = time()

    path_cd = r'Data\modisaqua'
    cd_files = sorted(os.listdir(path_cd))

    coords_lon = pd.read_csv(r'Dimensioncsv/Degree_0.04.csv')
    co_lon = coords_lon['degree004'][:].to_numpy()
    dict = {}
    for c in co_lon[:]:
        range_start = float(c.split('~')[0])  # Extract the range start value
        # Convert the range start value to an integer key
        lonl = int(range_start * 100)
        if(lonl % 4 == 1):
            lonl = lonl - 1
        if(lonl % 4 == 3):
            lonl = lonl + 1
        dict[lonl] = c

    point_lat = pd.read_csv(r'Dimensioncsv/medi_lat.csv')
    point_lon = pd.read_csv(r'Dimensioncsv/medi_lon.csv')

    points_lat = point_lat['lat'][:5].to_numpy()
    points_lon = point_lon['lon'][:5].to_numpy()

    # Create a grid of points
    grid_lat, grid_lon = np.meshgrid(points_lat, points_lon)

    # Flatten the grid to get 1D arrays of latitudes and longitudes
    points_lat_flat = grid_lat.flatten()
    points_lon_flat = grid_lon.flatten()

    # Create the points array from the flattened latitude and longitude arrays
    points = np.array([points_lat_flat, points_lon_flat]).T

    grid = 0.04166

    # Calculate ilats and ilons for each point in the grid
    ilats = np.abs(np.floor((90 - points_lat_flat) / grid)).astype(int)
    ilons = np.abs(np.floor((-180 - points_lon_flat) / grid)).astype(int)

    # Create the index_cd list from the ilats and ilons arrays
    index_cd = list(zip(ilats, ilons))

    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    with Pool() as pool:
        pool.map(process_file, [(file, path_cd, index_cd, points, dict, connection_string) for file in cd_files])

    end_time = time()

    return end_time - start_time

def database_connection(config_file_name):
    ''' This Function will connect to the database'''
    with open('config.txt', 'r') as file:
        connection_info = {}
        for line in file:
            key, value = line.strip().split('=')
            connection_info[key] = value

    conn = psycopg2.connect(
        database=connection_info['database'],
        user=connection_info['user'],
        password=connection_info['password'],
        host=connection_info['host'],
        port=connection_info['port']
    )

    conn.autocommit = True
    cursor = conn.cursor()

    return conn,cursor, connection_info
if __name__ == "__main__":

    st = time()
    conn,cursor, connection_info = database_connection(config_file_name='config.txt')
    print('Loading data into modisaqua table\n')
    run_time_path = load_modisaqua(connection_info)
    print(f"Time to insert into modisaqua = {run_time_path}s")
    
    # calling fact tables constraints function 
    constraints_of_facts.fact_constraints(cursor)

    # calling the analyze fact tables function
    analyze_table.analyze_fact_tables(connection_info)
    
    conn.close()