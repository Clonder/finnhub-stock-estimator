import argparse
from queue import Queue
from threading import Thread

from Spark.ModelWrapper.ModelWrapper import ModelWrapper
# Dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import train_test_split
from sklearn.model_selection import TimeSeriesSplit
from pathlib import Path

# Matplotlib: https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.html
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import time
import datetime as dt
import pathlib


def input_file_scanner_task(args, output_queue: Queue):
    print("Starting input file scanner task")
    while True:
        time.sleep(1)
        for file in pathlib.Path(args.input_path).glob('*.csv'):
            data = pd.read_csv(file, index_col='Date', parse_dates=False)
            output_queue.put((file.name, data))
            file.unlink()
            print(f"Added file data to the queue")

def main(args):
    mw = ModelWrapper(args.model_path)
    dataQueue = Queue()
    inputTask = Thread(target=input_file_scanner_task, args=(args, dataQueue))
    inputTask.start()

    while True:
        (name, data) = dataQueue.get(block=True)
        date_index = data.index
        data = mw.lstm_scale(data)
        X1, y1 = mw.lstm_split(data.values, n_steps=10)
        y = mw.predict(data)
        plot_stock_data_test(date_index[9:], y1, y, args.output_path + f"/{name}.png")
        data.to_csv(args.output_path + f"/{name}.csv")

def plot_stock_data_test(x_dates, y_test, y_pred, output_path):
    # Plot the data
    plt.figure(figsize=(15, 10))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=60))
    plt.plot(x_dates, y_test, label='y_test')
    plt.plot(x_dates, y_pred, label='y_pred')
    plt.xlabel('Date Time')
    plt.ylabel('Scaled Value')
    plt.legend()
    plt.gcf().autofmt_xdate()
    plt.savefig(output_path)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

if __name__ == "__main__":
    print("Starting Predicter on Spark")
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-path',
                        default=r'./input')
    parser.add_argument('-o', '--output-path',
                        default=r'./output')
    parser.add_argument('-m', '--model-path',
                        default=r'./lstm_model.keras')
    args = parser.parse_args()
    main(args)

