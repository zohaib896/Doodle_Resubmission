# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
import pandas as pd
import json


# Initialize producer variable and set parameter for JSON encode
producer2 = KafkaProducer(bootstrap_servers=
                         ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
