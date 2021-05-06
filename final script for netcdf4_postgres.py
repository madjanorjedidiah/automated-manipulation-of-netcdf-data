#!/usr/bin/env python
# coding: utf-8

# In[1]:


from netCDF4 import Dataset
from matplotlib import pyplot as plt # import libraries
import pandas as pd
import xarray as xr
import netCDF4
import numpy as np
import geopandas as gpd
from shapely.geometry import Point # Shapely for converting latitude/longtitude to geometry
import os
import numpy.ma as ma
import psycopg2


# In[2]:


#get all nc data
def get_nc():
    file_list=[]
    for file in os.listdir('/home/jed/Downloads/india'):
        if file.endswith(".nc4"):
            file_list.append(file)
            
    return file_list   
print(get_nc())


# In[3]:


#load only selected nc data
def load_nc(file_name):
    nc = Dataset(file_name, 'r')
    return nc


# In[4]:


#select a dataset based on index,visualize with print
nc_data = get_nc()[1]
#print (nc_data)


# In[5]:


#load selected dataset, visualize with print
dataset = load_nc(nc_data)
# print (a)


# In[6]:


#load selected variables,
dataset.variables.keys()


# In[15]:


#visualize variable properties
dataset.variables['longitude']  #where longitude is key from the above


# In[18]:


#create list of variables you want to select
variables = ['latitude', 'longitude', 'surface_temperature', 'temperature_at_5m']


#fubction that will siaplay selected variables in a table
def select_var_data(array, time=''):
    df = pd.DataFrame(columns = array)
    for b in array:
        df[b] = dataset.variables[b][:]
        
    df['source_name'] = dataset.title
    times = dataset.variables[time]
    tt = netCDF4.num2date(times[:],times.units)
    df[time] = tt
    return df.head(15)   

#uncomment the below script to visualize dataframe
select_var_data(variables, time='time')


# In[12]:


#export dataframe to csv
csv = select_var_data(variables).to_csv('fisrt_try.csv', header=True, index=False, sep=',')


# In[13]:


# connect to default database
conn = psycopg2.connect(host="localhost", port = 5432, database="imgtxt", user="postgres", password="postgres1234")

# Create a cursor object
cur = conn.cursor()


# In[ ]:


# Insert DataFrame records one by one.
# spurs1 is my postgres table name and the headers i want to insert the data into are as follows.
# the headers should follow the way the dataframe headers have been aligned or else the will be a mix of data.
#if you look carefully I have 4 colums in my dataframe and have selected 4 columns from my postgres table and they follow

for i,row in select_var_data(variables).iterrows():
    sql = "INSERT INTO spurs1 (lat, lon, temp, air_pressure, source_name) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cur.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    conn.commit()

