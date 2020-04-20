# confluent_kafka_multi_queuing

Implementation of multi queuing using confluent kafka

### Requirements (Windows env)

1. Docker (or u can have ur kafka or zookeeper running in ur local)
2. python 3.6 or >
3. java

### Procedure

Proceed with these steps once all the requirements are setup in ur system and u r gonna use docker
1. clone the repo 
2. open cmd in the specific location
3. pip install -r requirements.txt
4. docker-compose up -d --build
5. docker-compose ps (see if the status is up)
6. run main.py

### input

topic_name message/data key partition

##### eg: 

test_1 foo_bar hell 0
