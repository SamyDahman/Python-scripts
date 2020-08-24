import pymongo
from pymongo import MongoClient
import json
from bson.objectid import ObjectId
import requests
client = MongoClient("<access string>")
db = client["<database>"]
collection = db["<collection>"]

url = 'https://api.ximilar.com/tagging/fashion/v2/tags'

headers={'Authorization': "<Token>",
         'Content-Type': 'application/json'}


def tagImages():
    query = collection.find({}, {"images": 1, "_id": 1});
    num = 0
    for doc in query:
        id = doc["_id"]
        firstImageURL = doc["images"][0]
        secondImageURL = doc["images"][1]
        data = {
            'records': [{"_url": firstImageURL}],
            'task_id': "<Task id>"
        }
        response = requests.post(url, headers=headers, data = json.dumps(data))
        x = response.text
        responseDict = json.loads(x)
        tagsLocal = responseDict["records"][0]["_tags_simple"]
        q = {"_id" : ObjectId(id)}
        collection.update_one(q, { "$set": { "tags" : tagsLocal }})
        if num == 0:
            break

tagImages()
