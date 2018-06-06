#!/usr/bin/env python3
import json
import pandas as pd
import numpy as np
import keras
from keras.utils import plot_model
from keras.models import Sequential
from keras.layers import Dense
from sklearn.model_selection import train_test_split
from keras.utils import to_categorical
observations = pd.read_pickle('data/pickle/observations.pkl')
#with open('data/json/lookup.json') as f:
#    lu = json.load(f)
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
model.add(Dense(units=64, activation='relu', input_dim=144))
model.add(Dense(units=5, activation='softmax'))
model.compile(loss='sparse_categorical_crossentropy',
              optimizer='sgd',
              metrics=['accuracy'])



y_train_binary = to_categorical(y_train)
y_test_binary = to_categorical(y_test)

model.fit(x_train, y_train, epochs=5, batch_size=32)
#model.fit(x_train, y_train, epochs=144, batch_size=32)
loss_and_metrics = model.evaluate(x_test, y_test_binary, batch_size=128)
classes = model.predict(x_test, batch_size=128)


#  encounters[row.encounter_id,stage_col] = row.stage


