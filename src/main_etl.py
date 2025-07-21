import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.mongo_config import get_database
from config.supabase_config import upload_dataframe_to_supabase


def etl_users():
    db = get_database()
    users_collection = db["users"]

    docs = list(users_collection.find())
    df = pd.DataFrame(docs)

    df['_id'] = df['_id'].astype(str)
    df.drop(columns=['password'], inplace=True, errors='ignore')
    df.rename(columns={'_id': 'user_id'}, inplace=True)
    df.drop(columns=['preferences'], inplace=True, errors='ignore')


    upload_dataframe_to_supabase(df, "users")


def etl_movies():
    db = get_database()
    movies_collection = db["movies"]

    docs = list(movies_collection.find())

    def transform_movie_doc(doc):
        return {
            "title": doc.get("title"),
            "year": doc.get("year"),
            "runtime": doc.get("runtime"),
            "genres": ', '.join(doc.get("genres", [])),
            "languages": ', '.join(doc.get("languages", [])),
            "directors": ', '.join(doc.get("directors", [])),
            "imdb_rating": doc.get("imdb", {}).get("rating"),
            "imdb_votes": doc.get("imdb", {}).get("votes"),
            "released": pd.to_datetime(doc.get("released"), errors='coerce'),
        }

    transformed_docs = [transform_movie_doc(doc) for doc in docs]
    df = pd.DataFrame(transformed_docs)

    upload_dataframe_to_supabase(df, "movies")


if __name__ == "__main__":
    print(f"[{datetime.now()}] Starting ETL pipeline...")
    try:
        etl_users()
        print("[✓] Users ETL complete.")
        etl_movies()
        print("[✓] Movies ETL complete.")
    except Exception as e:
        print(f"[✗] ETL failed: {e}")
