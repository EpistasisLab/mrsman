#!/usr/bin/env python3
import pandas as pd
import keras
from keras.utils import plot_model
from keras.models import Sequential
from keras.layers import Dense
from sklearn.model_selection import train_test_split
from keras.utils import to_categorical

observations = pd.read_pickle('data/pickle/observations.pkl');
#test/train split
x_train, x_test, y_train, y_test = train_test_split(observations.drop('stage',1).values,observations.stage.values,  test_size=0.4, random_state=0)



model = Sequential()
model.add(Dense(units=64, activation='relu', input_dim=5))
model.add(Dense(units=32, activation='softmax'))
model.compile(loss='categorical_crossentropy',
              optimizer='sgd',
              metrics=['accuracy'])



y_train_binary = to_categorical(y_train)
y_test_binary = to_categorical(y_test)

model.fit(x_train, y_train_binary, epochs=5, batch_size=32)
loss_and_metrics = model.evaluate(x_test, y_test_binary, batch_size=128)
classes = model.predict(x_test, batch_size=128)
