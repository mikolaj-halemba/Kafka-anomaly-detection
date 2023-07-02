import json
import os
import socket
import logging
from typing import Optional
from confluent_kafka import Message, Consumer
from kafka_client import KafkaClient

class KafkaConsumer(KafkaClient):
    """
    The KafkaConsumer class is used to create consumers for Apache Kafka.

    Attributes:
        config_json (dict): Configuration parameters for the consumer, loaded from a JSON file.
        consumer (Consumer): The Kafka consumer object.
    """
    def __init__(self, config_path: str, group_id: str, topic: str):
        """
        The constructor for KafkaConsumer class.

        Args:
            config_path (str): The path to the JSON configuration file.
            group_id (str): The group ID of the consumer.
            topic (str): The topic to subscribe to.
        """
        self.config_json = self._get_config(config_path)
        self.consumer = self._create_consumer(self.config_json, group_id, topic)

    def _create_consumer(self, config_json: dict, group_id: str, topic: str) -> Consumer:
        """
        Create a Kafka consumer using the configuration parameters.

        Args:
            config_json (dict): The configuration parameters.
            group_id (str): The group ID of the consumer.
            topic (str): The topic to subscribe to.

        Returns:
            Consumer: The Kafka consumer object, or None if an error occurred.
        """
        try:
            consumer = Consumer({
              "bootstrap.servers": config_json["KAFKA_BROKER"],
              "group.id": group_id,
              "client.id": socket.gethostname(),
              "isolation.level": config_json["ISOLATION_LEVEL"],
              "default.topic.config":{
                        "auto.offset.reset": config_json["AUTO_OFFSET_RESET"],
                        "enable.auto.commit": False
                }

            })
            consumer.subscribe([topic])
        except Exception as e:
            logging.exception("Can't open consumer" + str(e))
            consumer = None

        return consumer
    
    def close(self) -> None:
        """
        Close the consumer.
        """
        self.consumer.close()
    

    def poll(self) -> Optional[Message]:
        """
        Poll the consumer for new messages.
        """
        return self.consumer.poll(timeout=1.0)
