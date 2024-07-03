import os
import json
import ast
import websocket
from dotenv import load_dotenv 
from utilities import load_avro_schema, avro_encode
import finnhub
from kafka import KafkaProducer

class FinnhubProducer:
    def __init__(self, api_token, kafka_server, kafka_port, kafka_topic_name, stocks_tickers):
        # Initializing Finnhub client and Kafka producer
        self.finnhub_client = finnhub.Client(api_key=api_token)
        self.producer = KafkaProducer(bootstrap_servers=f"{kafka_server}:{kafka_port}")
        # Loading Avro schema
        self.avro_schema = load_avro_schema('./finnhub_producer/src/schemas/trades.avsc')
        self.tickers = stocks_tickers.split(',')

        # Setting up websocket connection to Finnhub API
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={api_token}',
                              on_message=self.on_message,
                              on_error=self.on_error,
                              on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    # Callback function for receiving messages from websocket
    def on_message(self, ws, message):
        message = json.loads(message)
        # Encoding message using Avro schema and sending it to Kafka topic
        try:
            avro_message = avro_encode(
                {
                    'data': message['data'],
                    'type': message['type']
                }, 
                self.avro_schema
            )
            self.producer.send(os.environ['KAFKA_TOPIC_NAME'], avro_message)
        except:
            print(e)
        print(message)
        

    # Error handling callback function
    def on_error(self, ws, error):
        print(error)

    # Callback function for websocket close event
    def on_close(self, wsf):
        print("closed")

    # Callback function for websocket open event
    def on_open(self, ws):
        # Subscribing to tickers when websocket connection is opened
        for ticker in self.tickers:
            print(ticker)
            self.ws.send(f'{{"type":"subscribe","symbol":"{ticker}"}}')

# Function to load configuration from environment variables
def load_config():
    load_dotenv()  # Loading environment variables from .env file
    api_token = os.getenv('FINNHUB_API_TOKEN')
    kafka_server = os.getenv('KAFKA_SERVER')
    kafka_port = os.getenv('KAFKA_PORT')
    kafka_topic_name = os.getenv('KAFKA_TOPIC_NAME')
    stocks_tickers = os.getenv('FINNHUB_STOCKS_TICKERS')
    print(f"FINNHUB_API_TOKEN{api_token} KAFKA_SERVER{kafka_server} KAFKA_PORT{kafka_port}KAFKA_TOPIC_NAME{kafka_topic_name} FINNHUB_STOCKS_TICKERS{stocks_tickers}")
    return api_token, kafka_server, kafka_port, kafka_topic_name, stocks_tickers

if __name__ == "__main__":
    # Loading configuration
    api_token, kafka_server, kafka_port, kafka_topic_name, stocks_tickers = load_config()
    # Initializing FinnhubProducer with configuration
    FinnhubProducer(api_token, kafka_server, kafka_port, kafka_topic_name, stocks_tickers)
