from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

import time_uuid, uuid

# pip install kafka-python
# pip install time-uuid

KAFKA_TOPIC_NAME_CONS = "counts"   #topic
KAFKA_BOOTSTRAP_SERVERS_CONS = '127.0.0.1:9092'

if __name__ == "__ma _":
    print("Kafka Producer Application Started ... ")
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    message_list = []
    message = None
    for i in range(3600):
        i = i + 1
        date_today = datetime.now()
        message = {}
        print("Preparing message: " + str(i))
        #event_datetime = datetime.now()

        #message["order_id"] = i
        #message["uuid_id"] = str(uuid.uuid4())

        car = random.randint(0,4)
        bus = random.randint(0,2)
        truck = random.randint(0,2)
        jeepney = random.randint(0, 2)
        bike = random.randint(0, 5)
        tryke = random.randint(0, 3)
        others = random.randint(0, 2)

        total = car + bus + truck + jeepney + bike + tryke + others

        #send data
        message["timeuuid_id"] = str(time_uuid.utctime())
        message["lgu_code"] = '1200'
        message["sensor_id"] = 'sensor_01'
        message["date_saved"] = str(date_today.strftime('%m/%d/%Y'))
        message["time_saved"] = str(date_today.strftime("%X"))
        message["total"] = total
        message["car"] = car
        message["bus"] = bus
        message["truck"] = truck
        message["jeepney"] = jeepney
        message["bike"] = bike
        message["tryke"] = tryke
        message["others"] = others


        print("Message: ", message)
        
        #message_list.append(message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)  #sleep every second

    # print(message_list)

    print("Kafka Producer Application Completed. ")