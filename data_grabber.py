import pandas as pd
import pymysql
from sqlalchemy import create_engine
containers_url = 'https://api.data.amsterdam.nl/afval/v1/containers/?detailed=1&format=csv&page_size=15000'
# containers_url = 'https://api.data.amsterdam.nl/afval/v1/containers/?detailed=1&format=csv'
wells_url = 'https://api.data.amsterdam.nl/afval/v1/wells/?format=csv&page_size=15000'
# wells_url = 'https://api.data.amsterdam.nl/afval/v1/wells/?format=csv&page_size=15'
container_types_url = 'https://api.data.amsterdam.nl/afval/v1/containertypes/?format=csv&page_size=15000'
db_engine = create_engine('mysql+pymysql://waste:wastepwd@localhost/waste')
# def get_postcode(street, housenumber, city):


print("Starting import...")

# Grab containers and create a smaller more relevant list. Fields can be added later if needed
container_list = pd.read_csv(containers_url)
# print(container_list.columns)
print("Cleaning containers")
containers = container_list[['id', 'active', 'container_type.id', 'waste_name', 'waste_type', 'well.id', 'placing_date']]
containers = containers.rename(columns={
    'id': 'container_id',
    'container_type.id': 'container_type_id',
    'well.id': 'well_id',
}).set_index('container_id')
container_list.to_csv('data/containers.csv')

# Import wells and store it in a smaller list.
print("Importing wells")
well_list = pd.read_csv(wells_url)
print("Cleaning wells and saving locally")
wells = well_list[['id', 'stadsdeel', 'buurt_code', 'geometrie.type', 'address.neighbourhood', 'address.summary', 'geometrie.coordinates.0', 'geometrie.coordinates.1']]
wells = wells.rename(columns={'id': 'well_id', 'geometrie.type': 'well_type', 'address.summary': 'well_address', 'address.neighbourhood': 'well_neighbourhood', 'geometrie.coordinates.0': 'well_longitude', 'geometrie.coordinates.1': 'well_latitude'}).set_index('well_id')
wells.to_csv('data/wells.csv')

# Import container types and store it in a smaller list.
print("Importing container types")
container_types = pd.read_csv(container_types_url)
print("Cleaning container types and saving locally")
container_types = container_types[['id', 'name', 'volume', 'weight']]
container_types = container_types.rename(columns={
    'id': 'container_type_id'
}).set_index('container_type_id')
container_types.to_csv('data/container_types.csv')
# store to db
# print(well_list.columns)
print("Storing data in database....")
containers.to_sql('containers', db_engine, if_exists='replace', index='container_id')
container_types.to_sql('container_types', db_engine, if_exists='replace', index='container_type_id')
wells.to_sql('wells', db_engine, if_exists='replace', index='well_id')
print("Done!")
