"""
This script runs a Kafka producer that generates and sends data to a specified Kafka topic.
"""

import argparse
import json
import time
from dateutil.parser import parse
import socket
import json
import os
import time 
import logging
from datetime import datetime
from numpy.random import uniform, choice, randn
from random import random as r
import numpy as np
from kafka_producer import KafkaProducer

def main():
    """
    Main function that sets up and runs the Kafka producer.
    """
    LAG = 0.5
    PROBABILITY_OUTLIER = 0.05    
    _id = 0
    
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str, help='Name of the Kafka topic to stream.')
    args = parser.parse_args()
    TRANSACTION_TOPIC = args.topic
    
    producer = KafkaProducer('kafka_producer_config.json')
    
    if producer is not None:
        while True:
            if r() <= PROBABILITY_OUTLIER:
                X_test = uniform(low=-4, high=4, size=(1,2))
            else:
                X = 0.3 * randn(1,2)
                X_test = (X + choice(a=[2,-2], size=1, p=[0.5, 0.5]))

            X_test = np.round(X_test, 3).tolist()
            current_time = datetime.utcnow().isoformat()

            record = {
            "id": _id,
            "data": X_test,
            "current_time" : current_time
            }
            record = json.dumps(record).encode("utf-8")
            print("Send data:", record)
            producer.produce(topic=TRANSACTION_TOPIC, value=record)

            producer.flush()
            _id +=1 
            time.sleep(LAG)
            
    producer.close()
    
if __name__ == "__main__":
    main()
