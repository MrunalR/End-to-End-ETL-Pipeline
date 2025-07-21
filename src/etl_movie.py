import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.mongo_config import get_database
from config.supabase_config import upload_dataframe_to_supabase

def transform_movie_doc(doc):
    def get_nested(d, keys, default=None):
        for key in keys:
            d = d.get(key, {})
        return d if d else default

    return {
        "title": doc.get("title"),
        "year": doc.get("year"),
        "runtime": doc.get("runtime"),
        "genres": ", ".join(doc.get("genres", [])),
        "cast_list": ", ".join(doc.get("cast", [])),
        "languages": ", ".join(doc.get("languages", [])),
        "countries": ", ".join(doc.get("countries", [])),
        "directors": ", ".join(doc.get("directors", [])),
        "rated": doc.get("rated"),
        "plot": doc.get("plot"),
        "poster": doc.get("poster"),
        "released": pd.to_datetime(doc.get("released"), errors='coerce'),
        "lastupdated": pd.to_datetime(doc.get("lastupdated", None), errors='coerce'),
        "awards_text": get_nested(doc, ["awards", "text"]),
        "awards_wins": get_nested(doc, ["awards", "wins"]),
        "awards_nominations": get_nested(doc, ["awards", "nominations"]),
        "imdb_rating": get_nested(doc, ["imdb", "rating"]),
        "imdb_votes": get_nested(doc, ["imdb", "votes"]),
        "imdb_id": get_nested(doc, ["imdb", "id"]),
        "tomatoes_viewer_rating": get_nested(doc, ["tomatoes", "viewer", "rating"]),
        "tomatoes_viewer_reviews": get_nested(doc, ["tomatoes", "viewer", "numReviews"]),
        "tomatoes_critic_rating": get_nested(doc, ["tomatoes", "critic", "rating"]),
        "tomatoes_critic_reviews": get_nested(doc, ["tomatoes", "critic", "numReviews"]),
        "tomatoes_meter": get_nested(doc, ["tomatoes", "critic", "meter"]),
        "tomatoes_last_updated": pd.to_datetime(get_nested(doc, ["tomatoes", "lastUpdated"]), errors='coerce')
    }

# Step 1: Connect to MongoDB and fetch data
db = get_database()
movies_collection = db["movies"]
docs = list(movies_collection.find().limit(20))

# Step 2: Transform documents
transformed_docs = [transform_movie_doc(doc) for doc in docs]
df_movies = pd.DataFrame(transformed_docs)

# Step 3: Upload to Supabase
upload_dataframe_to_supabase(df_movies, "movies")
