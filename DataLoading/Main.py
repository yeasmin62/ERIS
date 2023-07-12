import psycopg2
import create_tables
import constraints_of_dimensions
import constraints_of_facts
import load_dimensions
import load_climate_fact_table
import load_copernicus_fact_table
import load_modisaqua_fact_table
import load_pathfinder_fact_table

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
    conn, cursor = database_connection(config_file_name='config.txt')
    # print(conn)

    # calling database creation function
    create_tables.table_creation(conn, cursor)

    # calling dimension contstrains function
    constraints_of_dimensions.dim_constraints(conn, cursor)

    # calling fact tables constraints function
    constraints_of_facts.fact_constraints(conn,cursor)

    # calling loading dimension function
    load_dimensions.load_dim(conn, cursor)

    # calling all the functions of loading data
    load_climate_fact_table.load_climate(conn,cursor)
    load_copernicus_fact_table.load_copernicus(conn,cursor)
    load_modisaqua_fact_table.load_modsaqua(conn,cursor)
    load_pathfinder_fact_table.load_pathfinder(conn, cursor)

    # closing the database connection
    conn.close()
