import pandas as pd
import pymysql
from sqlalchemy import create_engine
import requests
import re
from time import sleep
from sys import exit

url = "https://bwnr.nl/postcode.php" # ?st=Reimerswaalstraat&hn=22&pl=Amsterdam&tg=data&ac=adres2pc"


def get_postal_code(street, house_number):
    data = requests.get(url, params={'st': street, 'hn': house_number, 'pl': 'Amsterdam', 'tg': 'data', 'ac': 'adres2pc'})
    postal_code = data.text.split(';')[0]
    return postal_code


# remote_db = create_engine('mysql+pymysql://student:IHisCool!@34.77.233.175/waste')

address_list = pd.read_csv('data/wells.csv')
grabbed = 0
unique_address = address_list.well_address.unique()
# print(unique_address)
for a in unique_address:
    if len(address_list.loc[address_list['well_address'] == a, 'postal_code']) == 0:
        clean = re.sub(', Amsterdam', '', a)
        h_n = re.findall('\d+', clean)[0]
        address = re.findall('\D+', clean)[0].strip()
        postal_code = get_postal_code(address, h_n)
        if postal_code == "Over quota.":
            exit()
        print("Address:", address, "House Number:", h_n, "Postal Code:", postal_code)
        address_list.loc[address_list['well_address'] == a, 'postal_code'] = postal_code
        # UPDATE CSV AT EVERY RUN IN CASE IT CRASHES
        # address_list.to_csv('wells', remote_db, if_exists='replace', index='well_id')
        address_list.to_csv('data/wells.csv')
    else:
        print("Record Skipped")
    grabbed += 1
    print("Got", grabbed, "out of", len(unique_address))
    sleep(0.5)
print("DONE!!!")
# address_list.to_csv('data/wells.csv')
