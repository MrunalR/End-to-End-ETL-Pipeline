# mongo_config.py
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

def get_mongo_client():
    return MongoClient(MONGO_URI)

def get_database():
    return get_mongo_client()[DB_NAME]
