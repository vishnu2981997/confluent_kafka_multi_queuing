"""

"""
import json
import sys

from confluent_kafka import Consumer, KafkaError, TopicPartition

from custom_logging import log


def main():
    """
    main
    :return: None
    """
    log(CONSUMER_DETAILS, CONSUMER_DETAILS["topic"])

    c = Consumer(CONSUMER_DETAILS["config"])
    topic = TopicPartition(CONSUMER_DETAILS["topic"], int(CONSUMER_DETAILS["partition"]))

    c.assign([topic])
    try:
        while True:
            msg = c.poll(0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            log('Received message: {}'.format(msg.value().decode('utf-8')), file_name=CONSUMER_DETAILS["topic"])
    except Exception as exe:
        log(exe, CONSUMER_DETAILS["topic"])
        log(CONSUMER_DETAILS, CONSUMER_DETAILS["topic"])
        c.close()


if __name__ == "__main__":
    CONSUMER_DETAILS = {
        "topic": str(sys.argv[1]),
        "config": json.loads(sys.argv[2])["config"],
        "partition": str(sys.argv[3])
    }
    main()
