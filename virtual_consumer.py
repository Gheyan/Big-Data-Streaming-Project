# -*- coding: utf-8 -*-

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'cctv_vehicle_counts',         #cctv_vehicle_counts
    bootstrap_servers = ['127.0.0.1:9092'],
    auto_offset_reset = 'latest',
    enable_auto_commit = True
    )

for message in consumer:
    message = message.value
    print(message)
