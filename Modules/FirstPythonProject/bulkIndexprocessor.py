from elasticsearch import Elasticsearch
import json

# Opening JSON file
f = open('data.json')

# returns JSON object as
# a dictionary
data = json.load(f)


# Iterating through the json
# list
print(data)
es = Elasticsearch("http://localhost:9200")
for number in range(103, 1001):
    res = es.index(index="big_payload2", id=number, document=data)
    print(number)

# Closing file
f.close()
