import pandas as pd
import json

filelocation = 'C:/kafka/kafka_2.11-0.9.0.1/stream.json'
def data_generator():
  for line  in open(filelocation ,encoding="utf-8") :
    #x= json.dumps([fp.readline()])
    reader = json.loads(line)
    yield reader
