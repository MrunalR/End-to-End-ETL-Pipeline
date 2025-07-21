import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.mongo_config import get_database
from config.supabase_config import upload_dataframe_to_supabase

# Step 1: Connect to MongoDB
db = get_database()
users_collection = db["users"]
docs = list(users_collection.find().limit(5))

# Step 2: Transform to DataFrame
df = pd.DataFrame(docs)
df['_id'] = df['_id'].astype(str)
df.drop(columns=['password'], inplace=True, errors='ignore')
df.rename(columns={'_id': 'user_id'}, inplace=True)

# Step 3: Load to Supabase
upload_dataframe_to_supabase(df, "users")
