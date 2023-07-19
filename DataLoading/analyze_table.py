from sqlalchemy import create_engine, text

def analyze_dimension_tables(connection_info):
    dim_table_names = ['dates', 'months', 'semiday', 'resolution001', 'resolution004', 'resolution005', 'resolution020']
    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        for table_name in dim_table_names:
            query = text(f"ANALYZE {table_name};")
            conn.execute(query)

def analyze_fact_tables(connection_info):
    fact_table_names = ['climate_temperature', 'copernicus_temperature', 'pathfinder_temperature', 'modisaqua_temperature']
    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        for table_name in fact_table_names:
            query = text(f"ANALYZE {table_name};")
            conn.execute(query)