# Example python program to read data from a PostgreSQL table

# and load into a pandas DataFrame

import psycopg2

import pandas as pds

from sqlalchemy import create_engine

 

# Create an engine instance

alchemyEngine   = create_engine('postgresql+psycopg2://hubertstinia:@127.0.0.1', pool_recycle=3600);

 

# Connect to PostgreSQL server

dbConnection    = alchemyEngine.connect();

 

# Read data from PostgreSQL database table and load into a DataFrame instance

dataFrame       = pds.read_sql("select * from \"generation\"", dbConnection);

 

pds.set_option('display.expand_frame_repr', False);

 

# Print the DataFrame

print(dataFrame);

 

# Close the database connection

dbConnection.close();