# Dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
import pandas as pd
import numpy as np
from pathlib import Path

# Matplotlib: https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.html
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import datetime as dt

# Tensorflow: https://www.tensorflow.org/api_docs/python/tf
from tensorflow.keras.models import load_model
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout
from tensorflow.keras.optimizers import *
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import train_test_split
from sklearn.model_selection import TimeSeriesSplit

import pymongo
import time

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["database"]



DEFAULT_MODEL_PATH = './predictor/lstm_model.keras'
DEFAULT_CHUNK_PATH = './home/ubuntu/finnhub_stock_estimator/chunk'
DEFAULT_PROCESSED_PATH = './processed'
TRAIN = False
DEBUG = False

def main():
    while True:
    	predict()
    	time.sleep(600)


def predict():
    print("start")
    for chunk in Path(DEFAULT_CHUNK_PATH).glob('*.csv'):
            print(f"chunk:{chunk}")
            try:
                column_names=["ingest_timestamp","price",'symbol','trade_timestamp',"uuid","volume"]
                input = pd.read_csv(chunk, parse_dates=False, names=column_names)
                print(input.head())                
                symobols = ['GOOGL', 'AAPL', 'MSFT']
                
                for symbol in symobols:
                    google = input[input['symbol'] == symbol]
                    #times = pd.to_datetime(input['trade_timestamp'])
                    google['trade_timestamp'] = pd.to_datetime(google['trade_timestamp'])
                    google = google[['trade_timestamp', 'price']]

                    stock_data = google.groupby(pd.Grouper(key='trade_timestamp', freq='1S')).agg(['min', 'max', 'first', 'last']) \
                        .dropna().droplevel(0, axis=1) \
                        .rename(columns={'min': 'low', 'max': 'high',
                                         'first': 'open', 'last': 'close'}).reset_index()

                    #low = google.groupby(['symbol', times.dt.hour, times.dt.minute, times.dt.second]).price.min()
                    #high = google.groupby(['symbol', times.dt.hour, times.dt.minute, times.dt.second]).price.max()
                    #open = google.groupby(['symbol', times.dt.hour, times.dt.minute, times.dt.second]).price.mean()
                    #close = google.groupby(['symbol', times.dt.hour, times.dt.minute, times.dt.second]).price.mean()

                    #stock_data = pd.DataFrame({'open': open.values[:], 'high': high.values[:], 'low': low.values[:], 'close': close.values[:]}).reset_index()


                    print(stock_data.head())
                    if DEBUG:
                        plot_stock_data(stock_data)

                    X_feat = stock_data.iloc[:, 1:5]

                    sc = StandardScaler()
                    X_ft = sc.fit_transform(X_feat.values)
                    stock_data_ft = pd.DataFrame(columns=X_feat.columns, data=X_ft, index=X_feat.index)

                    X1, y1 = lstm_split(stock_data_ft.values, n_steps=10)

                    train_split = 1
                    date_index = stock_data.trade_timestamp


                    print(X1.shape, X1.shape, y1.shape)

                    lstm = load_model(DEFAULT_MODEL_PATH)

                    y_pred = lstm.predict(X1).ravel()
                    rmse = mean_squared_error(y1, y_pred, squared=False)
                    mape = mean_absolute_percentage_error(y1, y_pred)
                    print('RMSE: ', rmse)
                    print('MAPE: ', mape)
                    export = pd.DataFrame({'trade_timestamp': date_index[9:], 'y_test': y1, 'y_pred': y_pred})
                    if DEBUG:
                        plot_stock_data_test(date_index[9:], y1, y_pred)
                        export.to_csv(f'./{symbol}_output.csv')
                    else:
                        mycol = mydb[symbol]
                        mycol.insert_many(export.to_dict('records'))
                Path(chunk).rename(f"{DEFAULT_PROCESSED_PATH}/{chunk.name}")
            except Exception as e:
                print(e)



def train_lstm(X_train, y_train):
    lstm = Sequential()
    lstm.add(LSTM(50, activation='relu', input_shape=(X_train.shape[1], X_train.shape[2]), return_sequences=True))
    lstm.add(LSTM(50, activation='relu'))
    lstm.add(Dense(1))
    lstm.compile(loss='mean_squared_error', optimizer='adam')
    print(lstm.summary())
    history = lstm.fit(X_train, y_train, epochs=100, batch_size=4, verbose=1, shuffle=False)
    lstm.save(DEFAULT_MODEL_PATH)


def lstm_split(data, n_steps):
    X, y = [], []
    for ic in range(len(data) - n_steps + 1):
        X.append(data[ic:ic + n_steps, :-1])
        y.append(data[ic + n_steps-1 , -1])
    return np.array(X), np.array(y)


def plot_stock_data(stock_data):
    # Plot the data
    plt.figure(figsize=(15, 10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=60))
    x_dates = stock_data.trade_timestamp

    plt.plot(x_dates, stock_data['high'], label='High')
    plt.plot(x_dates, stock_data['low'], label='Low')
    plt.xlabel('Date Time')
    plt.ylabel('Scaled Value')
    plt.legend()
    plt.gcf().autofmt_xdate()
    plt.show()


def plot_stock_data_test(x_dates, y_test, y_pred):
    # Plot the data
    plt.figure(figsize=(15, 10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=60))

    plt.plot(x_dates, y_test, label='y_test')
    plt.plot(x_dates, y_pred, label='y_pred')
    plt.xlabel('Date Time')
    plt.ylabel('Scaled Value')
    plt.legend()
    plt.gcf().autofmt_xdate()
    plt.show()


if __name__ == '__main__':
    main()
