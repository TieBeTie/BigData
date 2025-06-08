#!/usr/bin/env python3
"""
Setup script: Clear ratings table and load Fantasy movie ratings + regular ratings
"""

import os
import csv
from google.cloud import spanner

# Configuration
PROJECT_ID = "ramatreemany"
INSTANCE_ID = "demo-spanner-instance"
DATABASE_ID = "movielens-db"
RATINGS_FILE = "../local_dataset/movielens/ratings_20m.csv"
MOVIES_FILE = "../local_dataset/movielens/movies.csv"

def setup_service_account():
    """Set up Google Cloud authentication"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

def get_fantasy_movie_ids():
    """Get movie IDs for Fantasy genre movies"""
    fantasy_movies = set()
    
    with open(MOVIES_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            genres = row['genres']
            if 'Fantasy' in genres:
                fantasy_movies.add(int(row['movieId']))
                if len(fantasy_movies) >= 50:  # Limit to first 50 Fantasy movies
                    break
    
    print(f"Found {len(fantasy_movies)} Fantasy movies: {sorted(list(fantasy_movies))[:10]}...")
    return fantasy_movies

def clear_and_load_ratings(database):
    """Clear ratings table and load Fantasy + regular ratings"""
    
    # Step 1: Clear existing ratings
    def delete_all_ratings(transaction):
        transaction.execute_update("DELETE FROM ratings WHERE true")
    
    database.run_in_transaction(delete_all_ratings)
    print("Cleared all existing ratings")
    
    # Step 2: Get Fantasy movie IDs
    fantasy_movie_ids = get_fantasy_movie_ids()
    
    # Step 3: Read ratings from CSV and collect Fantasy + regular ratings
    fantasy_ratings = []
    regular_ratings = []
    
    with open(RATINGS_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            user_id = int(row['userId'])
            movie_id = int(row['movieId'])
            rating = float(row['rating'])
            timestamp = int(row['timestamp'])
            
            rating_tuple = (user_id, movie_id, rating, timestamp)
            
            if movie_id in fantasy_movie_ids:
                fantasy_ratings.append(rating_tuple)
                if len(fantasy_ratings) >= 100:  # Stop when we have 100 Fantasy ratings
                    break
            elif len(regular_ratings) < 50:  # Also collect some regular ratings
                regular_ratings.append(rating_tuple)
    
    # Combine all ratings
    all_ratings = fantasy_ratings + regular_ratings
    
    print(f"Collected {len(fantasy_ratings)} Fantasy ratings and {len(regular_ratings)} regular ratings")
    print(f"Total ratings to load: {len(all_ratings)}")
    
    # Step 4: Insert data in batches
    with database.batch() as batch:
        batch.insert(
            table="ratings",
            columns=("userId", "movieId", "rating", "timestamp"),
            values=all_ratings
        )
    
    print(f"Loaded {len(all_ratings)} ratings into Spanner")
    
    # Verify data - use separate snapshots
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT COUNT(*) FROM ratings")
        count = list(results)[0][0]
        print(f"Verification: {count} ratings in table")
        
    # Show Fantasy sample - use new snapshot
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("""
            SELECT r.userId, r.movieId, r.rating, m.title, m.genres
            FROM ratings r
            JOIN movies m ON r.movieId = m.movieId
            WHERE m.genres LIKE '%Fantasy%'
            LIMIT 5
        """)
        print("Fantasy ratings sample:")
        for row in results:
            print(f"  User {row[0]}, Movie {row[1]} ({row[3]}), Rating: {row[2]}")
            
    # Count Fantasy ratings
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("""
            SELECT COUNT(*) 
            FROM ratings r
            JOIN movies m ON r.movieId = m.movieId
            WHERE m.genres LIKE '%Fantasy%'
        """)
        fantasy_count = list(results)[0][0]
        print(f"Total Fantasy ratings loaded: {fantasy_count}")

def main():
    """Main setup function"""
    setup_service_account()
    
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    
    clear_and_load_ratings(database)
    print("Setup completed successfully!")

if __name__ == "__main__":
    main() 