from pydoc_data.topics import topics
from confluent_kafka import Producer
import os
import logging
import websocket
from confluent_kafka.error import KafkaException
import json

def deliver_message(err, msg):
    if err is not None:
        logging.error("Message delivery failed: {}".format(err))
    else:
        logging.info("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))



def on_message(ws, message):
    # Ensure message is passed to producer
    if isinstance(message, bytes):
        message = message.decode('utf-8',errors="strict")

    # Now message is a str
    data = json.loads(message)
    if data.get("type") == "trade":
        print(data)
        producer.produce(topic="cryptocurrency", value=message, key="d", callback=deliver_message)

def on_error(ws, error):
    logging.error("Error happened while extracting data: " + str(error))

def on_close(ws):
    logging.error("### Socket closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

def test_kafka_connection(bootstrap_servers):
    try:
        logging.info(f"Testing connection to Kafka cluster at: {bootstrap_servers}")

        # Create a producer to test connection
        producer = Producer({'bootstrap.servers': bootstrap_servers})

        # Request metadata
        metadata = producer.list_topics(timeout=5)

        logging.info("======= ✅ Connection successful! ====================================================\n")
        logging.info(f"Available topics: {list(metadata.topics.keys())}")

        return True
    except KafkaException as e:
        logging.error("❌ Kafka connection failed: " + str(e))
        return False
    except Exception as e:
        logging.error("===== ❌ Unexpected error:====================================================\n" + str(e))
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(message)s:%(levelname)s:%(name)s')
    address='kafka:9092'
    test_kafka_connection(address)
    # Initialize the Kafka producer
    producer = Producer({
        'bootstrap.servers':address,
    })
    #
    topic = "cryptocurrency"
    token = os.getenv('API_KEY')
    print(token)


    # WebSocket connection setup
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token=d0308dhr01qi6jgjnug0d0308dhr01qi6jgjnugg",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.on_open = on_open

    # Run the WebSocket connection in the background
    ws.run_forever()
