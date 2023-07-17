import psycopg2
import create_tables
import constraints_of_dimensions
import constraints_of_facts
import load_dimensions
import load_climate_fact_table
import load_copernicus_fact_table
import load_modisaqua_fact_table
import load_pathfinder_fact_table
import schema_insertion

def database_connection(config_file_name):
    ''' This Function will connect to the database'''
    with open(config_file_name, 'r') as file:
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

    conn.autocommit = True
    cursor = conn.cursor()

    return conn, cursor


if __name__ == "__main__":

    with open('config.txt', 'r') as file:
        connection_info = {}
        for line in file:
            key, value = line.strip().split('=')
            connection_info[key] = value

    conn, cursor = database_connection(config_file_name='config.txt')
    # print(conn)

    # calling database creation function
    create_tables.table_creation(conn, cursor)

    # calling dimension contstrains function
    constraints_of_dimensions.dim_constraints(conn, cursor)

    # calling fact tables constraints function
    constraints_of_facts.fact_constraints(conn,cursor)

    #calling schema Insertion function
    schema_insertion.schemaInsert(conn,cursor)


    # calling loading dimension function
    load_dimensions.load_dim(connection_info)

    # calling all the functions of loading data
    # run_time = load_climate_fact_table.load_climate(connection_info)
    print('Loading data into climate\n')
    run_time = load_climate_fact_table.load_climate(connection_info)
    print(f"Time to insert into climate = {run_time}s")

    print("Loading data into copernicus\n")
    run_time_copernicus = load_copernicus_fact_table.load_copernicus(connection_info)
    print(f'loading time is {run_time_copernicus}s')

    print('Loading data into modisaqua table\n')
    run_time_modis = load_modisaqua_fact_table.load_modsaqua(connection_info)
    print(f'Loading time {run_time_modis}')
    
    print('Loading data into pathfinder table\n')
    run_time_path = load_pathfinder_fact_table.load_pathfinder(connection_info)
    print(f"Time to insert into pathfinder = {run_time_path}s")

    # closing the database connection
    conn.close()
