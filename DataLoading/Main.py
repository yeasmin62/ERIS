import psycopg2
from sqlalchemy import create_engine
import create_tables
import constraints_of_dimensions
import constraints_of_facts
import load_dimensions
import load_climate_fact_table
import load_copernicus_fact_table
import load_modisaqua_fact_table
import load_pathfinder_fact_table
import schema_insertion
import analyze_table
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


    conn,cursor, connection_info = database_connection(config_file_name='config.txt')
    # print(conn)

    # calling database creation function
    create_tables.table_creation(cursor)


    #calling schema Insertion function
    schema_insertion.schemaInsert(cursor)


    # calling loading dimension function
    load_dimensions.load_dim(connection_info)

    # calling dimension contstrains function
    constraints_of_dimensions.dim_constraints(cursor)

    # calling the analyze dimension tables function
    analyze_table.analyze_dimension_tables(connection_info)

    
    
    # calling all the functions of loading data
    # run_time = load_climate_fact_table.load_climate(connection_info)
    print('Loading data into climate\n')
    run_time = load_climate_fact_table.load_climate(connection_info)
    print(f"Time to insert into climate = {run_time}s")

    print("Loading data into copernicus\n")
    run_time_copernicus = load_copernicus_fact_table.load_copernicus(connection_info)
    print(f'loading time is {run_time_copernicus}s')

    print('Loading data into modisaqua table\n')
    run_time_modis = load_modisaqua_fact_table.load_modisaqua(connection_info)
    print(f'Loading time {run_time_modis}')
    
    print('Loading data into pathfinder table\n')
    run_time_path = load_pathfinder_fact_table.load_pathfinder(connection_info)
    print(f"Time to insert into pathfinder = {run_time_path}s")
    
    # calling fact tables constraints function
    constraints_of_facts.fact_constraints(cursor)

    # calling the analyze fact tables function
    analyze_table.analyze_fact_tables(connection_info)



    # closing the database connection
    conn.close()
