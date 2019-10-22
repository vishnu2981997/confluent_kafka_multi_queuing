"""

"""
import json
import os
import subprocess

from confluent_kafka import Producer, admin

from custom_logging import log


class CustomKafka:
    """
    Custom Kafka class
    """

    def __init__(self, config):
        """
        Constructor
        :param config: dict
        """
        self.__config = config

    def initialize_admin(self):
        """
        Creates an kafka admin object
        :return: kafka admin object
        """
        adm = admin.AdminClient(self.__config)
        return adm

    def initialize_producer(self):
        """
        Creates a kafka producer object
        :return: kafka producer object
        """
        p = Producer(self.__config)
        return p

    def produce(self, p, topic, data, key, partition):
        """
        Push a message to kafka based on given details
        :param p: Kafka producer object
        :param topic: string
        :param data: dict
        :param key: string
        :param partition: int
        :return: None
        """
        p.produce(topic, data.encode('utf-8'), key=key, partition=partition, callback=self.delivery_report)

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    @staticmethod
    def create_topic(topic, config, num_partitions=0):
        """
        Creates a kafka topic based on given data
        :param topic: string
        :param config: dict
        :param num_partitions: int
        :return: None
        """
        admin.NewTopic(topic=topic, num_partitions=num_partitions, config=config)

    @staticmethod
    def create_partition(topic, num_partitions):
        """
        Creates partitions for a given kafka topic
        :param topic: string
        :param num_partitions: int
        :return: None
        """
        admin.NewPartitions(topic=topic, new_total_count=num_partitions)

    @staticmethod
    def initialize_consumer(topic, config, partition=0):
        """
        Initializes a Kafka consumer for a given topic
        :param topic: string
        :param config: dict
        :param partition: int
        :return: None
        """
        config = {"config": config}
        log(file_name=topic, msg="strted")
        path = os.path.abspath('kafka_consumer.py')
        config = json.dumps(config).replace(": ", ":")
        p = subprocess.Popen(['python', path, topic, config, str(partition)], shell=True, stdin=None, stdout=None,
                             stderr=None, close_fds=True)
