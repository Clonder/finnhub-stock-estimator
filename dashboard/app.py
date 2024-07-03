import streamlit as st
import pandas as pd
import yfinance as yf
from pymongo import MongoClient
import plotly.graph_objects as go
from datetime import datetime

st.title('Stock Price Predictions')
st.sidebar.info('Welcome to the Stock Price Prediction')

db_name = "database"
collection_name = "collection"


# Sidebar for user input (stock symbol)
stock_symbol = st.sidebar.text_input("Enter Stock Symbol (e.g., GOOGL, AAPL, MSFT)", "GOOGL")
today = datetime.today()


start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2010-01-01"))
end_date = st.sidebar.date_input("End Date", value=today, max_value=today)
t1 = st.sidebar.time_input("Start Time", value=None)
t2 = st.sidebar.time_input("End Time", value=None)


#default_date_yesterday = today - timedelta(hours=1)
#start_pred_time = st.time_input(value=default_date_yesterday, key=None, help=None, on_change=None, args=None, kwargs=None, *, disabled=False, label_visibility="visible", step=00:10:00)
#end_pred_time = st.time_input(value="now", key=None, help=None, on_change=None, args=None, kwargs=None, *, disabled=False, label_visibility="visible", step=0:10:00)


# Fetch historical stock data
try:
    stock_data = yf.download(stock_symbol, start=start_date, end=end_date)

    # Create plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=stock_data.index, y=stock_data['Close'], mode='lines', name='Close Price'))
    fig.update_layout(title=f'Historical Stock Prices for {stock_symbol}', xaxis_title='Date', yaxis_title='Price')

    # Display plot
    st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error fetching data: {e}")

# Function to connect to MongoDB and fetch data
def get_data_from_mongo(db_name, collection_name, stock_symbol):
#    Uncomment the following lines to connect to a MongoDB instance
    client = MongoClient('mongodb://localhost:27017/')  # TODO Replace with MongoDB connection string
    db = client[db_name]
    query={}
    cursor = collection = db[stock_symbol].find(query)
    data = pd.DataFrame(list(cursor))
    data=data[(data['trade_timestamp'] > str(start_date)) & (data['trade_timestamp'] < str(end_date))]
    data=data.groupby(['trade_timestamp'], as_index=False)[["y_test","y_pred"]].mean()
    print(data)
    return data
    #return pd.read_csv('dashboard/mock_data.csv')

# Sidebar selection for stocks
predicted_stock = st.sidebar.selectbox(
    "Choose a stock for prediction",
    ("GOOGL", "AAPL", "MSFT")
)

# Load the appropriate DataFrame based on the stock selection
if predicted_stock == "GOOGL":
    df = get_data_from_mongo(db_name, collection_name, "GOOGL")
elif predicted_stock == "AAPL":
     df = get_data_from_mongo(db_name, collection_name, "AAPL")
elif predicted_stock == "MSFT":
    df = get_data_from_mongo(db_name, collection_name, "MSFT")
    
df['error'] = abs(df['y_test'] - df['y_pred'])


# Streamlit app title
st.markdown("## **Stock Prediction**")

# Create a plot for the stock prediction
fig = go.Figure(
    data=[
        go.Scatter(
            x=df['trade_timestamp'],
            y=df["y_test"],
            name="Actual",
            mode="lines",
            line=dict(color="blue"),
        ),
        go.Scatter(
            x=df['trade_timestamp'],
            y=df["y_pred"],
            name="Predicted",
            mode="lines",
            line=dict(color="red"),
        )
    ]
)

fig.update_layout(title=f'Historical Stock Prices for {predicted_stock}', xaxis_title='Date', yaxis_title='Price')
# Customize the stock prediction graph
fig.update_layout(xaxis_rangeslider_visible=False)

# Use the native streamlit theme.
st.plotly_chart(fig, use_container_width=True)
