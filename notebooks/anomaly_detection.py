import argparse
import json
import os
import socket
import time
import logging
from datetime import datetime
from dateutil.parser import parse
from numpy.random import uniform, choice, randn
from random import random as r
import numpy as np
from joblib import load
from kafka_producer import KafkaProducer
from kafka_consumer import KafkaConsumer
from multiprocessing import Process


def main(TRANSACTION_TOPIC, TRANSACTION_CG, ANOMALY_TOPIC):
    """
    Main function that initializes Kafka producer and consumer, and runs the anomaly detection model.

    Args:
        TRANSACTION_TOPIC (str): Name of the Kafka topic to stream transaction data.
        TRANSACTION_CG (str): Consumer group ID for the transaction data topic.
        ANOMALY_TOPIC (str): Name of the Kafka topic to stream anomaly data.
    """
    MODEL_PATH = os.path.abspath('isolation_forest.joblib')
    
    producer = KafkaProducer('kafka_producer_config.json')
    consumer = KafkaConsumer('kafka_consumer_config.json', TRANSACTION_TOPIC, TRANSACTION_CG)
    clf = load(MODEL_PATH)
    while True:
        message = consumer.poll()
        if message is None:
            continue
        if message.error():
            logging.error(f"CONSUMER error: {message.error()}")
            continue

        record = json.loads(message.value().decode('utf-8'))
        data = record['data']
        prediction = clf.predict(data)

        if prediction[0] == -1 :
            score = clf.score_samples(data)
            record["score"] = np.round(score, 3).tolist()

            _id = str(record["id"])
            record = json.dumps(record).encode("utf-8")
            print("Anomaly detected:", record)
            producer.produce(topic=ANOMALY_TOPIC, value=record)

            producer.flush()

    consumer.close()
    

if __name__ == "__main__":
    """
    Main entry point of the application. The script takes four command-line arguments: 
    the number of partitions, the name of the Kafka topic for transaction data, the consumer 
    group ID for the transaction data topic, and the name of the Kafka topic for anomaly data. 

    It creates the specified number of processes that run the 'main' function concurrently.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('NUM_PARTITIONS', type=str, help='Number of partitions.')
    parser.add_argument('TRANSACTION_TOPIC', type=str, help='Name of the Kafka topic to stream transaction data.')
    parser.add_argument('TRANSACTION_CG', type=str, help='Consumer group ID for the transaction data topic.')
    parser.add_argument('ANOMALY_TOPIC', type=str, help='Name of the Kafka topic to stream anomaly data.')
    args = parser.parse_args()

    for _ in range(int(args.NUM_PARTITIONS)):
        p = Process(target=main, args=(args.TRANSACTION_TOPIC, args.TRANSACTION_CG, args.ANOMALY_TOPIC))
        p.start()