import queue
from threading import Thread

# Dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
import pandas as pd
import numpy as np



# Tensorflow: https://www.tensorflow.org/api_docs/python/tf
from tensorflow.keras.models import load_model
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
from tensorflow.keras.optimizers import *
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import root_mean_squared_error
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import train_test_split
from sklearn.model_selection import TimeSeriesSplit
from pathlib import Path

INITIAL_TRAINING_DATA = '../data/NFLX-add.csv'
DEFAULT_MODEL_PATH = './lstm_model.keras'
TRAIN = False


class ModelWrapper():
    def __init__(self, path=DEFAULT_MODEL_PATH):
        path = Path(path)

        self.lstm = Sequential()
        if path.is_file():
            self.lstm = load_model(DEFAULT_MODEL_PATH)
        else:
            self.lstm = self.train_lstm_with_default()

    def lstm_scale(self, data):
        X_feat = data.iloc[:, 0:4]
        sc = StandardScaler()
        X_ft = sc.fit_transform(X_feat.values)
        stock_data_ft = pd.DataFrame(columns=X_feat.columns, data=X_ft, index=X_feat.index)
        return stock_data_ft

    def train_lstm_with_default(self) -> Sequential:
        stock_data = pd.read_csv(INITIAL_TRAINING_DATA, index_col='Date', parse_dates=False)
        print("Performing initial training with default data")
        print(stock_data.head())

        stock_data_ft = self.lstm_scale(stock_data)

        X1, y1 = self.lstm_split(stock_data_ft.values, n_steps=10)

        train_split = 0.8
        split_idx = int(np.ceil(len(X1) * train_split))
        date_index = stock_data_ft.index

        X_train, X_test = X1[:split_idx], X1[split_idx:]
        y_train, y_test = y1[:split_idx], y1[split_idx:]
        X_train_date, X_test_date = date_index[:split_idx], date_index[split_idx:]

        print(X1.shape, X_train.shape, X_test.shape, y_test.shape)

        lstm = Sequential()

        lstm = self.train_lstm(X_train, y_train)
        lstm = load_model(DEFAULT_MODEL_PATH)

        y_pred = lstm.predict(X_test).ravel()
        rmse = root_mean_squared_error(y_test, y_pred)
        mape = mean_absolute_percentage_error(y_test, y_pred)
        print('RMSE: ', rmse)
        print('MAPE: ', mape)

        return lstm

    def train_lstm(self, X_train, y_train):
        lstm = Sequential()
        lstm.add(LSTM(50, activation='relu', input_shape=(X_train.shape[1], X_train.shape[2]), return_sequences=True))
        lstm.add(LSTM(50, activation='relu'))
        lstm.add(Dense(1))
        lstm.compile(loss='mean_squared_error', optimizer='adam')
        print(lstm.summary())

        history = lstm.fit(X_train, y_train, epochs=100, batch_size=4, verbose=1, shuffle=False)
        lstm.save(DEFAULT_MODEL_PATH)

    def lstm_split(self, data, n_steps):
        X, y = [], []
        for ic in range(len(data) - n_steps + 1):
            X.append(data[ic:ic + n_steps, :-1])
            y.append(data[ic + n_steps - 1, -1])

        return np.array(X), np.array(y)

    def get_data_queue(self):
        return self.dataQueue

    def start_training_task(self):
        self.dataQueue = queue.Queue()
        self.commandQueue = queue.Queue()
        training_thread = Thread(target=self.training_thread, args=(self.dataQueue, self.commandQueue))
        training_thread.start()
        pass

    def add_training_data(self, data: pd.DataFrame):
        self.dataQueue.put(data)
        pass

    def training_thread(self, dataQueue: queue.Queue, commandQueue: queue.Queue):
        print("Training thread started")
        while True:
            data = [dataQueue.get()]
            print("Data received")
            while dataQueue.qsize() > 0:
                data.append(dataQueue.get())
                print("Data pack received")
            data = pd.concat(data)
            data = self.lstm_scale(data)
            X1, y1 = self.lstm_split(data.values, n_steps=10)
            try:
                history = self.lstm.fit(X1, y1, epochs=25, batch_size=4, verbose=1, shuffle=False)
                print("Model trained")
                self.lstm.save(DEFAULT_MODEL_PATH)
                print("Model saved")
            except Exception as e:
                print("Error training model")
                print(e)
        pass

    def predict(self, data: pd.DataFrame):
        X1, y1 = self.lstm_split(data.values, n_steps=10)
        y_pred = self.lstm.predict(X1).ravel()
        return y_pred
