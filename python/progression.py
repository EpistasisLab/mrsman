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
#q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE'})--(e:Encounter) with e match (p:Patient)--(e)--(o:Observation)--(c:Concept) where c.class = 'Test' or o.display = 'CURRENT WHO HIV STAGE' Return p.id as patient_id,e.id as encounter_id,o.display as observation,o.value as value,o.timestamp as timestamp order by p.id,o.timestamp"
#q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE'})--(e:Encounter) with e match (p:Patient)--(e)--(o:Observation)--(c:Concept)  Return p.id as patient_id,e.id as encounter_id,o.display as observation,o.value as value,o.timestamp as timestamp order by p.id,o.timestamp"
q="MATCH (o:Observation {display:'CURRENT WHO HIV STAGE'})--(e:Encounter) with e match (p:Patient)--(e)--(o:Observation) Return p.id as patient_id,e.id as encounter_id,o.display as observation,o.value as value,o.timestamp as timestamp order by p.id,o.timestamp"

results = db.query(q, data_contents=True)
columns = results.columns;
stages = {}
observations = pd.DataFrame.from_records(results.rows,columns=columns)

print("create lookup table")
lookup = {}
for var in columns:
    i = 0
    vals = observations[var].unique()
    lookup[var] = {}
    for val in vals:
        try:
          f=float(val)
        except ValueError:
          i += 1
          lookup[var][val] = i

print("processing stages")
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


print("transform observations")
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
pid_col = 0
#timestamp column
ts_col = max_observation + 1
#staging column
stage_col_1 = lookup["observation"]["CURRENT WHO HIV STAGE"]
stage_col_2 = max_observation + 2
last_col = stage_col_2
#last_col = 3
#
encounters = np.zeros((max_encounter+1,max_observation+3))
for i,row in observations.iterrows():
  encounters[row.encounter_id,row.observation] = row.value
  encounters[row.encounter_id,pid_col] = row.patient_id
  encounters[row.encounter_id,ts_col] = row.timestamp
  encounters[row.encounter_id,stage_col_2] = row.stage

#delete 1st row
encounters  = np.delete(encounters,0,0)
#encounters = encounters[1:max_encounter+1,1:max_observation+3]
endpoints = encounters[...,stage_col_2]
#remove primary staging column
encounters  = np.delete(encounters,stage_col_1,1)
#remove calculated staging column
np.savetxt("data/csv/encounters.csv", encounters, delimiter=",")
encounters  = np.delete(encounters,-1,1)
np.savetxt("data/csv/endpoints.csv", endpoints, delimiter=",")

#obs2 = encounters[...,:last_col-1]
x_train, x_test, y_train, y_test = train_test_split(encounters,endpoints,  test_size=0.2, random_state=0)
model = Sequential()
model.add(Dense(units=17, activation='sigmoid', input_dim=encounters.shape[1]))
model.add(Dense(units=np.unique(endpoints).size, activation='sigmoid'))
model.compile(loss='sparse_categorical_crossentropy',
              optimizer='sgd',
              metrics=['accuracy'])
y_train_binary = to_categorical(y_train)
y_test_binary = to_categorical(y_test)
history=model.fit(x_train, y_train, epochs=60, batch_size=100, validation_data=(x_test, y_test), verbose=2, shuffle=False)
#model.fit(x_train, y_train, epochs=144, batch_size=32)
loss_and_metrics = model.evaluate(x_test, y_test, batch_size=128)
classes = model.predict(x_test, batch_size=128)
pyplot.plot(history.history['loss'], label='train')
pyplot.plot(history.history['val_loss'], label='test')
pyplot.legend()
pyplot.show()

print("fin")
