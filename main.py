import pymongo
from pandas_ods_reader import read_ods
import pandas as pd
import json

with open('password.txt') as f:
    connection_url = f.read()

client = pymongo.MongoClient(eval(connection_url))

# Database
Database = client.get_database('mongodb')
# Table
Pessoas = Database.Pessoas
Pessoas.drop()

# Inserindo dados
path = "pessoas/pessoas.ods"
df = read_ods(path)
pessoas = []
for i in range(0,len(df)):
  individuo = json.loads(df.iloc[i].to_json())
  individuo.update({'_id':i})
  pessoas.append(individuo)
Pessoas.insert_many(pessoas)
