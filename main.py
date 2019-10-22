"""

"""
import json
import os

from custom_kafka import CustomKafka


def start_exited_consumers(kafka, p):
    """
    starts consumers for applications that are already created based on the topics.json
    :param kafka: Kafka object
    :param p: kafka producer object
    :return: None
    """
    for i in TOPICS["data"]:
        kafka.initialize_consumer(topic=i["topic"], config=i["config"], partition=int(i["partition"]))


def get_variable_data():
    """
    Gets data from topics.json
    :return: json data
    """
    path = os.path.abspath('topics.json')
    with open(path) as json_data_file:
        data = json.load(json_data_file)
    return data


def main():
    """
    Main
    :return: None
    """
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test',
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    }
    kafka = CustomKafka(conf)
    p = None
    flag = 0
    start_exited_consumers(kafka=kafka, p=p)
    try:
        p = kafka.initialize_producer()
        flag = 1
    except Exception as exe:
        print("smthin went wrong ", exe)
        exit()
    if flag == 1:
        try:
            while True:
                topic, data, key, partition = [i for i in input().split()]
                p.poll(0)
                new_topic_config = {
                    'bootstrap.servers': 'localhost:9092',
                    'group.id': 'test',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    }
                }
                flag = 0
                try:
                    kafka.create_topic(topic=topic, config=new_topic_config)
                    path = os.path.abspath('topics.json')
                    consumer_config = {
                        'bootstrap.servers': 'localhost:9092',
                        'group.id': 'test',
                        'default.topic.config': {
                            'auto.offset.reset': 'smallest'
                        }
                    }
                    if topic not in TOPIC_NAMES:
                        kafka.initialize_consumer(topic=topic, config=consumer_config, partition=0)
                        TOPICS["data"].append({"topic": topic, "config": consumer_config, "partition": str(partition)})
                        with open(path, 'w', encoding='utf-8') as file:
                            json.dump(TOPICS, file, ensure_ascii=False, indent=4)
                    flag = 1
                except Exception as exe:
                    print("smthin went wrong ", exe)
                if flag == 1:
                    kafka.produce(p=p, topic=topic, data=data, key=key, partition=int(partition))
                else:
                    print("error while creating topic")
        except Exception as exe:
            print("smthin went wrong ", exe)
        finally:
            p.flush()


if __name__ == "__main__":
    TOPICS = get_variable_data()
    TOPIC_NAMES = [i["topic"] for i in TOPICS["data"]]
    main()
