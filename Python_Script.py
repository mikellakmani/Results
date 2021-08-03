#import required packages
import pandas as pd
import requests
from sqlalchemy import create_engine

#Retrieve WB data using API. There are 6 pages so create a loop to retrieve the data
#from each page and concatenate the data
dfs = []

for i in range(1, 7):
    r = requests.get("http://api.worldbank.org/v2/country?format=json&page=" + str(i))

    df = pd.json_normalize(r.json()[1])
    dfs.append(df)

Countries = pd.concat(dfs, ignore_index=True)

#Create ENGINE and use sqlalchemy to import the data into SQL
ENGINE = create_engine('postgresql://postgres:password@localhost/MUDANO')

GDPData = pd.read_csv(r'/Users/mikellakmani/desktop/API_NY/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_2708436.csv',skiprows=4,dtype=str)
GDPData.to_sql(name='GDPData', con=ENGINE, if_exists= 'replace', index=False)

Countries.to_sql(name='Countries', con=ENGINE, if_exists= 'replace', index=False)


