#!/usr/bin/env python3
"""
Setup script for Spanner Task 1: Non-blocking reads demonstration
Creates Spanner instance, database, tables and loads MovieLens data from HDFS
"""

import subprocess
import csv
import os
from google.cloud import spanner
from datetime import datetime
import time

# Configuration
PROJECT_ID = "ramatreemany"
INSTANCE_ID = "demo-spanner-instance"
DATABASE_ID = "movielens-db"
LOCAL_MOVIES_PATH = "../local_dataset/movielens/movies.csv"
LOCAL_RATINGS_PATH = "../local_dataset/movielens/ratings_20m.csv"

def setup_service_account():
    """Set up Google Cloud authentication"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

def check_local_files():
    """Check if local MovieLens files exist"""
    import os
    
    if not os.path.exists(LOCAL_MOVIES_PATH):
        print(f"Error: {LOCAL_MOVIES_PATH} not found")
        return False
        
    if not os.path.exists(LOCAL_RATINGS_PATH):
        print(f"Error: {LOCAL_RATINGS_PATH} not found")
        return False
        
    print("✓ Local MovieLens files found")
    return True

def get_spanner_instance():
    """Get existing Spanner instance"""
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    return instance

def create_database_and_tables(instance):
    """Create database and tables"""
    database = instance.database(DATABASE_ID)
    
    if database.exists():
        print(f"Database {DATABASE_ID} already exists")
        return database
    
    # Database already exists
    return database

def parse_genres(genres_str):
    """Parse genres string - keep as string for existing schema"""
    if not genres_str or genres_str.strip() == '(no genres listed)':
        return ""
    return genres_str

def load_movies_data(database):
    """Load movies data from CSV"""
    movies_data = []
    with open(LOCAL_MOVIES_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie_id = int(row['movieId'])
            title = row['title']
            genres = parse_genres(row['genres'])
            movies_data.append((movie_id, title, genres))
    
    # Insert in batches
    batch_size = 1000
    with database.batch() as batch:
        for i in range(0, len(movies_data), batch_size):
            batch_data = movies_data[i:i + batch_size]
            batch.insert(
                table="movies",
                columns=("movieId", "title", "genres"),
                values=batch_data
            )

def load_ratings_data(database):
    """Load subset of ratings data from CSV"""
    ratings_data = []
    with open(LOCAL_RATINGS_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 10000:  # Limit to 10K for demo
                break
                
            user_id = int(row['userId'])
            movie_id = int(row['movieId'])
            rating = float(row['rating'])
            timestamp = int(row['timestamp'])  # Keep as INT64
            
            ratings_data.append((user_id, movie_id, rating, timestamp))
    
    # Insert in batches
    batch_size = 1000
    with database.batch() as batch:
        for i in range(0, len(ratings_data), batch_size):
            batch_data = ratings_data[i:i + batch_size]
            batch.insert(
                table="ratings",
                columns=("userId", "movieId", "rating", "timestamp"),
                values=batch_data
            )

def main():
    """Main setup function"""
    setup_service_account()
    
    if not check_local_files():
        return
    
    instance = get_spanner_instance()
    database = create_database_and_tables(instance)
    
    # Check if data exists
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT COUNT(*) FROM movies")
        movie_count = list(results)[0][0]
        
    if movie_count == 0:
        load_movies_data(database)
        load_ratings_data(database)
        print(f"Loaded data")
    else:
        print(f"Data already exists: {movie_count} movies")
    
    print("Setup completed")

if __name__ == "__main__":
    main() 