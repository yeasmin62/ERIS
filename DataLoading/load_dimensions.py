import os
import pandas as pd
from sqlalchemy import create_engine

def load_dim(connection_info):
    script_directory = os.path.dirname(os.path.abspath(__file__))  # taking the directory of the folder

    csv_files = {
        'dates': 'Dimensioncsv/Dates.csv',
        'months': 'Dimensioncsv/Months.csv',
        'semiday': 'Dimensioncsv/Semi-Day.csv',
        'resolution020': 'Dimensioncsv/Degree_0.20.csv',
        'resolution005': 'Dimensioncsv/Degree_0.05.csv',
        'resolution004': 'Dimensioncsv/Degree_0.04.csv',
        'resolution001': 'Dimensioncsv/Degree_0.01.csv',
    }
    connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
    engine = create_engine(connection_string)

    for table_name, csv_file in csv_files.items():
        df = pd.read_csv(os.path.join(script_directory, csv_file))
        df.to_sql(table_name, con=engine, if_exists='append', index=False)

  


