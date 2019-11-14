#!/usr/bin/env python3
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://localhost:7474", username="neo4j", password="password")
import pandas as pd
 
observations = pd.read_pickle('observations.pkl')
stages = pd.read_pickle('stages.pkl')
lookup = pd.read_pickle('lookup.pkl')

columns = observations.columns


def getstage(df):
   patient_id = df.patient_id
   timestamp = df.timestamp
   record = stages.query('patient_id == "'+ patient_id  +'" & timestamp == '+ str(timestamp))
   if(record.empty):
     return 0;
   else:
     return record.stage[0]

columns =  observations.columns
keys = {}
for column in observations.columns:
    keys[column] = {}

for look in lookup.iterrows():
    keys[look[1]['var']][look[1]['val']] = look[1]['index']

for i, row in o.iterrows():
  stage = getstage(row)
  o.at[i,'stage'] = stage
  for column in columns:
    try:
      val = keys[column][row[column]]
    except:
      val = row[column]
    o.at[i,column] = val
    
    
