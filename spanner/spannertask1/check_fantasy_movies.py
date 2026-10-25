#!/usr/bin/env python3
"""Check if we have Fantasy movies in our dataset"""

import os
from google.cloud import spanner

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

client = spanner.Client(project='ramatreemany')
instance = client.instance('demo-spanner-instance')
database = instance.database('movielens-db')

with database.snapshot() as snapshot:
    # Check which movies we have ratings for
    results = snapshot.execute_sql('SELECT DISTINCT movieId FROM ratings ORDER BY movieId LIMIT 10')
    movie_ids = [row[0] for row in results]
    print('Movie IDs in ratings:', movie_ids)
    
# Check if these movies have Fantasy genre
for movie_id in movie_ids[:5]:
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(f'SELECT title, genres FROM movies WHERE movieId = {movie_id}')
        movie_data = list(results)
        if movie_data:
            title, genres = movie_data[0]
            is_fantasy = 'Fantasy' in genres if genres else False
            print(f'Movie {movie_id}: {title} - Genres: {genres} - Fantasy: {is_fantasy}')

with database.snapshot() as snapshot:
    # Count Fantasy ratings directly
    results = snapshot.execute_sql("""
        SELECT COUNT(*) 
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE m.genres LIKE '%Fantasy%'
    """)
    count = list(results)[0][0]
    print(f'Total Fantasy movie ratings: {count}') 