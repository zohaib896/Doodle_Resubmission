# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
import sys
import json
import sqlite3
import producer_towrite_newtopic as p

# Initialize consumer variable and set property for JSON decode
consumer = KafkaConsumer ('my_topic90',bootstrap_servers = ['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')))
conn  =  sqlite3.connect('mydb.db')
cur= conn.cursor()


Consumed_user_recs= {}
for message in consumer:
    print("Consumer records:")
    print(message)
    #print("dic:", message[6])
    Consumed_user_recs =  message[6]
    print(Consumed_user_recs)

    ## counting and insertion into db
    for min , u  in  Consumed_user_recs.items() :
        usercount =  len(u)
        print(f'{min} - {usercount}')
        query = f'''INSERT OR Replace INTO UsersPerMinute (min, users) VALUES ({min}, {usercount}) ;'''
        try :
            cur.execute(query)
        except :
            raise Exception (' error failed executing insert query')
    #writing to new topic
    p.producer2.send('my_new_topic', Consumed_user_recs)
    conn.commit()

