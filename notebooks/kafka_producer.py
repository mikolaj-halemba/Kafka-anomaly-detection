import json
import os
import socket
import logging
from confluent_kafka import Producer
from kafka_client import KafkaClient


class KafkaProducer(KafkaClient):
    """
    The KafkaProducer class is used to create producers for Apache Kafka.

    Attributes:
        config_json (dict): Configuration parameters for the producer, loaded from a JSON file.
        producer (Producer): The Kafka producer object.
    """
    def __init__(self, config_path: str):
        
        """
        The constructor for KafkaProducer class.

        Args:
            config_path (str): The path to the JSON configuration file.
        """
        self.config_json = self._get_config(config_path)
        self.producer = self._create_producer(self.config_json)

    def _create_producer(self, config_json: dict) -> Producer:
        """
        Create a Kafka producer using the configuration parameters.

        Args:
            config_json (dict): The configuration parameters.

        Returns:
            Producer: The Kafka producer object, or None if an error occurred.
        """
        try:
            producer = Producer({
            "bootstrap.servers": config_json["KAFKA_BROKER"],
            "client.id": socket.gethostname(),
            "enable.idempotence": True,
            "batch.size": config_json["BATCH_SIZE"],
            "linger.ms": config_json["LINGER_MS"],
            "acks": config_json["ACKS"],
            "retries":config_json["RETRIES"],
            "delivery.timeout.ms": config_json["DELIVERY_TIMEOUT"],
            })
        except Exception as e:
            logging.exception("Can't create producer")
            producer = None
        return producer
    
    def produce(self, topic: str, value: str) -> None:
        """
        Produce a record to a Kafka topic.

        Args:
            topic (str): The name of the topic.
            value (str): The value to produce.
        """
        self.producer.produce(topic=topic, value=value)

    def flush(self) -> None:
        """
        Flush the producer's message queue.
        """
        self.producer.flush()
