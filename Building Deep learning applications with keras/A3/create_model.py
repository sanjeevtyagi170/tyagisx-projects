import pandas as pd
from keras.models import Sequential
from keras.layers import *

training_data_df = pd.read_csv("/workspaces/tyagisx/A3/sales_data_training_scaled.csv")

X = training_data_df.drop('total_earnings', axis=1).values
Y = training_data_df[['total_earnings']].values

print(training_data_df.columns)
# Define the model
model = Sequential()
model.add(Dense(50,input_dim=9,activation='relu'))
model.add(Dense(100,activation='relu'))
model.add(Dense(50,activation='relu'))
model.add(Dense(1,activation='linear'))
model.compile(loss="mse",optimizer='adam')
model.summary()
