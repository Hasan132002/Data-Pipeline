from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

db_name = "so_db"
collection_name = "so_sentiment"

db = client[db_name]

if collection_name in db.list_collection_names():
    print(f"Collection '{collection_name}' already exists in database '{db_name}'.")
else:
    db.create_collection(collection_name)
    print(f"Collection '{collection_name}' created successfully in database '{db_name}'.")

print("Collections in database:", db.list_collection_names())
