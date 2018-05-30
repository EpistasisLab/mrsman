#!/usr/bin/env python3
from neo4jrestclient import client
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://localhost:7474", username="neo4j", password="password")
import pandas as pd
import numpy as np
import json
import keras
from keras.utils import plot_model
from keras.models import Sequential
from keras.layers import Dense
from sklearn.model_selection import train_test_split
from keras.utils import to_categorical
from matplotlib import pyplot

 
print("query observations")
#q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE'})-[spawned]-(e:Encounter)-[encountered]-(p:Patient)-[encounter]-(e2:Encounter)-[]-(o2:Observation) Return p.id as patient_id,e2.id as encounter_id,o2.display as observation,o2.value as value,o2.timestamp as timestamp order by p.id,o2.timestamp"
q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE', value:'WHO STAGE 3 ADULT'})-[spawned]-(e:Encounter)-[encountered]-(p:Patient)-[]-(e2:Encounter)-[]-(ob:Observation  {display:'CURRENT WHO HIV STAGE'}) where not ob.value =  o.value  with p match (p)--(e3:Encounter)--(o3:Observation) Return distinct p.id as patient_id,e3.id as encounter_id,o3.display as observation,o3.value as value,o3.timestamp as timestamp order by p.id,o3.timestamp"

results = db.query(q, data_contents=True)
columns = results.columns;
stages = {}
print("processing observations")
for r in results:
    record = {}
    for i,val in enumerate(r):
        record[columns[i]] = val
    patient_id = record['patient_id']
    timestamp = record['timestamp']
    if (record['observation'] == 'CURRENT WHO HIV STAGE'):
        try:
            stage = int(record['value'][10])
        except:
            stage = 0
        if patient_id not in stages.keys():
           stages[patient_id] = {}
        if timestamp not in stages[patient_id].keys():
           stages[patient_id][timestamp] = stage
        #handle duplicates: use whatever stage isn't zero, otherwise last val
        elif stage != stages[patient_id][timestamp]:
           if stage == 0 & stages[patient_id][timestamp] != 0:
             newstage = stages[patient_id][timestamp]
           elif stage != 0 & stages[patient_id][timestamp] == 0:
             newstage = stage
           else:
             newstage = stage
           stages[patient_id][timestamp] = newstage

obs = pd.DataFrame.from_records(results.rows,columns=columns)

print("create lookup table")
lookup = {}
for var in columns:
    i = 0
    vals = obs[var].unique()
    lookup[var] = {}
    for val in vals:
        try:
          f=float(val)
        except ValueError:
          i += 1
          lookup[var][val] = i
    

def getstage(rec):
   patient_id = rec.patient_id
   timestamp = rec.timestamp
   if patient_id not in stages.keys():
     stage = 0
   elif timestamp not in stages[patient_id]:
     stage = 0
   else:
     stage = stages[patient_id][timestamp] 
   return stage


print("format staging")
observations = obs.copy();
observations['stage'] = 0;
for i, row in observations.iterrows():
  stage = getstage(row)
  observations.at[i,'stage'] = stage
  for column in columns:
    try: 
      val = lookup[column][row[column]]
    except:
      val = row[column]
    observations.at[i,column] = val

print("saving observations")
observations.to_csv('data/csv/observations.csv',index_label=False)
observations.to_pickle('data/pickle/observations.pkl')
with open('data/json/lookup.json', 'w') as outfile:
    json.dump(lookup, outfile)



print("analyzing")
max_encounter = observations.encounter_id.max()
max_observation = observations.observation.max()
#patient id column
pid_col = max_observation + 1
#timestamp column
ts_col = max_observation + 2
#staging column
stage_col = max_observation + 3
#
encounters = np.zeros((max_encounter+1,max_observation+4))
for i,row in observations.iterrows():
  encounters[row.encounter_id,row.observation] = row.value
  encounters[row.encounter_id,pid_col] = row.patient_id
  encounters[row.encounter_id,ts_col] = row.timestamp
  encounters[row.encounter_id,stage_col] = row.stage


#remove first row
encounters = encounters[1:]
#remove stage col
endpoints = encounters[...,stage_col]
obs = encounters[...,:stage_col]

x_train, x_test, y_train, y_test = train_test_split(obs,endpoints,  test_size=0.1, random_state=0)



model = Sequential()
model.add(Dense(units=64, activation='relu', input_dim=stage_col))
model.add(Dense(units=5, activation='softmax'))
model.compile(loss='sparse_categorical_crossentropy',
              optimizer='sgd',
              metrics=['accuracy'])



y_train_binary = to_categorical(y_train)
y_test_binary = to_categorical(y_test)

history=model.fit(x_train, y_train, epochs=15, batch_size=1, validation_data=(x_test, y_test), verbose=2, shuffle=False)
#model.fit(x_train, y_train, epochs=144, batch_size=32)
loss_and_metrics = model.evaluate(x_test, y_test, batch_size=128)
classes = model.predict(x_test, batch_size=128)
pyplot.plot(history.history['loss'], label='train')
pyplot.plot(history.history['val_loss'], label='test')
pyplot.legend()
pyplot.show()

print("fin")
