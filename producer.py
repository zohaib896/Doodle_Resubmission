# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
import pandas as pd
import json
import sqlite3
from  sqlalchemy import create_engine
import  collections
import MyGenerator as t

##PRAGMA journal_mode=WAL;
conn  =  sqlite3.connect('mydb.db')
cur= conn.cursor()
query  = '''create table  if not exists  UsersPerMinute (min int primary key , users int)'''
cur.execute(query)
conn.commit()

# Initialize producer variable and set parameter for JSON encode
producer = KafkaProducer(bootstrap_servers=
                         ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
lst =[]
dic =collections.defaultdict(list)
users= set()
df=pd.DataFrame()

for row in t.data_generator():
  #print(row)
    d= dict(row)
    df= pd.DataFrame(dict([(k, pd.Series(v)) for k, v in d.items()]))
    df = df[['uid' , 'ts']]
    df['ts'] = pd.to_datetime(df['ts'], unit='ms').apply(lambda x: x.to_datetime64())
    df['year'] = df['ts'].dt.year
    df['month'] = df['ts'].dt.month
    df['day'] = df['ts'].dt.day
    df['hour'] = df['ts'].dt.hour
    df['minute'] = df['ts'].dt.minute
    df['second'] = df['ts'].dt.second
    df=df.dropna()
    print (df.to_string(index = False))

    #x = df.groupby('year')['uid'].nunique()
    #print(x)
    for user , min in zip(df['uid'], df['minute']) :
        users.add(user)
        dic[min] = list(users)
        print(dic)
        producer.send('my_topic90', dic)
print("Message Sent to my_topic2")