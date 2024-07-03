from pymongo import MongoClient

# Initialize database and collection
client = MongoClient("mongodb://localhost:27017/")
db = client["database"]
collection = db["collection"]


print("Initialization completed.")
