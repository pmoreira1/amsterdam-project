import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

# URLS OF CSV FILES TO IMPORT
containers_url = 'https://api.data.amsterdam.nl/afval/v1/containers/?detailed=1&format=csv&page_size=15000'
wells_url = 'https://api.data.amsterdam.nl/afval/v1/wells/?format=csv&page_size=15000'
container_types_url = 'https://api.data.amsterdam.nl/afval/v1/containertypes/?format=csv&page_size=15000'

# DATABASE DETAILS TO SAVE INFORMATION AT END OF SCRIPT.
remote_db = create_engine('mysql+pymysql://student:IHisCool!@34.77.233.175/waste')

print("======STARTING IMPORT======")
# Working on Containers
print("Grabbing latest containers data...")
container_list = pd.read_csv(containers_url)
print("Cleaning containers data and saving locally")
containers = container_list[['id', 'active', 'container_type.id', 'waste_name', 'waste_type', 'well.id', 'placing_date']]
containers = containers.rename(columns={
    'id': 'container_id',
    'container_type.id': 'container_type_id',
    'well.id': 'well_id',
}).set_index('container_id')
container_list.to_csv('data/containers.csv')

# Working on Wells
print("Grabbing latest wells data")
well_list = pd.read_csv(wells_url)
print("Cleaning wells and saving locally")
wells = well_list[['id', 'stadsdeel', 'buurt_code', 'geometrie.type', 'address.neighbourhood', 'address.summary', 'geometrie.coordinates.0', 'geometrie.coordinates.1']]
wells = wells.rename(columns={'id': 'well_id', 'geometrie.type': 'well_type', 'address.summary': 'well_address', 'address.neighbourhood': 'well_neighbourhood', 'geometrie.coordinates.0': 'well_longitude', 'geometrie.coordinates.1': 'well_latitude'}).set_index('well_id')
wells['postal_code'] = np.nan
wells.to_csv('data/wells.csv')

# Working on Container Types
print("Grabbing latest container types")
container_types = pd.read_csv(container_types_url)
print("Cleaning container types and saving locally")
container_types = container_types[['id', 'name', 'volume', 'weight']]
container_types = container_types.rename(columns={
    'id': 'container_type_id'
}).set_index('container_type_id')
container_types.to_csv('data/container_types.csv')

# Working on Population Data
print("Grabbing latest population data")
popuplation_data = pd.read_excel('data/2020-stadsdelen.xlsx').reset_index()
print("Cleaning population data and saving locally")
popuplation_data = popuplation_data.rename(columns={
    'index': 'buurt_id',
    'wijk/std': 'buurt_code',
    'naam wijk/std': 'buurt'
}).dropna().set_index('buurt_id')
popuplation_data.to_csv('data/popuplation_data.csv')

# Saving information in database
print("Storing all data in database....")
containers.to_sql('containers', remote_db, if_exists='replace', index='container_id')
container_types.to_sql('container_types', remote_db, if_exists='replace', index='container_type_id')
wells.to_sql('wells', remote_db, if_exists='replace', index='well_id')
popuplation_data.to_sql('popuplation_data', remote_db, if_exists='replace', index='buurt_id')
print("======IMPORT COMPLETE======")
