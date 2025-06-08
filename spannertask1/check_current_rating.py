#!/usr/bin/env python3
"""Check current Fantasy movie rating"""

import os
from google.cloud import spanner

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

client = spanner.Client(project='ramatreemany')
instance = client.instance('demo-spanner-instance')
database = instance.database('movielens-db')

with database.snapshot() as snapshot:
    results = snapshot.execute_sql('''
        SELECT r.userId, r.movieId, r.rating, m.title, m.genres
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE m.genres LIKE "%Fantasy%"
    ''')
    for row in results:
        print(f'Fantasy rating: User {row[0]}, Movie {row[1]} ("{row[3]}"), Rating: {row[2]}') 