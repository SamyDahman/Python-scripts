#imports all packages
import json
import ijson
import pymongo
import time
from pymongo import MongoClient

t0 = time.time()

# Connect to Mongodb client
client = MongoClient("<access string>")
db = client["<database>"]
collection = db["<collection>"]

# ijson parse
file = open("<file path>", "r")
items = ijson.items(file, "item")
# Create arrays for inserting/filtering
itemsForInsert = []

# Counter for clarity
numItemsUpdated = 0

# Function to determine if item has stock
def has_stock(prod):
    for x in prod["inventory"]:
        for y in x["store_inventory"]:
            if y["bopisQuantity"] != "0":
                return True
    return False

# insert if item does not exist in database, update only inventory if it does
for item in items:
    if has_stock(item):
        prodId = item["product_id"]
        query = {"product_id": prodId}
        if type(item["price"]) != float and type(item["price"]) != str:
            item["price"] = float(item["price"])
            for x in item["inventory"]:
                for y in x["store_inventory"]:
                    if y["price"] is not str:
                        y["price"] = float(item["price"])
        if collection.count_documents(query, limit=1) == 0:
            itemsForInsert.append(item)
        else:
            collection.update_one(query, {"$set": {"inventory": item["inventory"]}})
            numItemsUpdated = numItemsUpdated + 1

# Print the number of items to be inserted
print("Items to be inserted: " + str(len(itemsForInsert)))

# Print number of items updated
print("Number of items updated: " + str(numItemsUpdated))

# Bulk insert
collection.insert_many(itemsForInsert)

# Calculate and print timer
t1 = time.time()
print(t1 - t0)
