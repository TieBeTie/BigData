#!/usr/bin/env python3
"""Check current data in Spanner tables"""

from google.cloud import spanner
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

client = spanner.Client(project='ramatreemany')
instance = client.instance('demo-spanner-instance')
database = instance.database('movielens-db')

with database.snapshot() as snapshot:
    # Check movies count
    results = snapshot.execute_sql('SELECT COUNT(*) FROM movies')
    movie_count = list(results)[0][0]
    print(f'Movies count: {movie_count}')
    
    # Check ratings count  
    results = snapshot.execute_sql('SELECT COUNT(*) FROM ratings')
    rating_count = list(results)[0][0]
    print(f'Ratings count: {rating_count}')
    
    # Sample data
    results = snapshot.execute_sql('SELECT * FROM movies LIMIT 3')
    print('\nMovies sample:')
    for row in results:
        print(f'  {row}')
        
    results = snapshot.execute_sql('SELECT * FROM ratings LIMIT 3')  
    print('\nRatings sample:')
    for row in results:
        print(f'  {row}')