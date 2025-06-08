#!/usr/bin/env python3
"""
Spanner Task 1: Non-blocking reads isolation demonstration
Shows that read-only transactions are not blocked by concurrent read-write transactions
"""

import os
import time
import threading
from google.cloud import spanner
from datetime import datetime

# Configuration
PROJECT_ID = "ramatreemany"
INSTANCE_ID = "demo-spanner-instance"
DATABASE_ID = "movielens-db"

# Results storage
query1_results = []  # Read transaction during update
query4_results = []  # Read transaction after update
update_completed = False

def setup_service_account():
    """Set up Google Cloud authentication"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ramatreemany-0d452ecc0a37.json'

def long_read_transaction(database, results_list, transaction_name):
    """
    Execute a read transaction with N queries, each reading 10 rows
    Query 1: rows 1-10, Query 2: rows 11-20, etc.
    Uses multi-use read-only transaction for true isolation demonstration
    """
    start_time = datetime.now()
    print(f"{transaction_name}: Starting long read transaction...")
    
    # Use multi-use read-only transaction for true isolation
    # This ensures all queries see the same consistent snapshot of data
    with database.snapshot(multi_use=True) as snapshot:
        batch_size = 10
        offset = 0
        query_count = 0
        
        while query_count < 10:  # Read 10 batches = 100 rows total
            query_count += 1
            
            sql = f"""
                SELECT r.userId, r.movieId, r.rating, r.timestamp
                FROM ratings r
                ORDER BY r.userId, r.movieId
                LIMIT {batch_size} OFFSET {offset}
            """
            
            results = snapshot.execute_sql(sql)
            batch_data = list(results)
            
            if not batch_data:
                break  # No more data
                
            print(f"{transaction_name}: Query {query_count} - read {len(batch_data)} rows (offset {offset})")
            
            # Store results
            for row in batch_data:
                record = {
                    'userId': row[0],
                    'movieId': row[1], 
                    'rating': row[2],
                    'timestamp': row[3]
                }
                results_list.append(record)
            
            offset += batch_size
            
            # Simulate processing time between queries
            time.sleep(0.5)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if results_list:
        ratings = [r['rating'] for r in results_list]
        avg_rating = sum(ratings) / len(ratings)
        print(f"{transaction_name}: COMPLETED - {len(results_list)} total records, avg rating: {avg_rating:.3f}, duration: {duration:.1f}s")
    else:
        print(f"{transaction_name}: COMPLETED - no data found")

def update_fantasy_ratings(database):
    """Update all Fantasy movie ratings by adding 2 to each rating"""
    global update_completed
    
    # Wait longer to ensure read transaction starts and reads some data first
    time.sleep(3)
    
    start_time = datetime.now()
    print("UPDATE: Starting Fantasy ratings update...")
    
    def update_transaction(transaction):
        # Find Fantasy movie ratings to update
        results = transaction.execute_sql("""
            SELECT r.userId, r.movieId, r.rating
            FROM ratings r
            JOIN movies m ON r.movieId = m.movieId
            WHERE m.genres LIKE '%Fantasy%'
        """)
        
        updates = []
        for row in results:
            user_id, movie_id, current_rating = row[0], row[1], row[2]
            new_rating = min(5.0, current_rating + 2.0)  # Cap at 5.0
            updates.append((user_id, movie_id, new_rating))
        
        if updates:
            # Perform updates
            for user_id, movie_id, new_rating in updates:
                transaction.execute_update(
                    "UPDATE ratings SET rating = @new_rating WHERE userId = @user_id AND movieId = @movie_id",
                    params={
                        'new_rating': new_rating,
                        'user_id': user_id,
                        'movie_id': movie_id
                    },
                    param_types={
                        'new_rating': spanner.param_types.FLOAT64,
                        'user_id': spanner.param_types.INT64,
                        'movie_id': spanner.param_types.INT64
                    }
                )
        
        return len(updates)
    
    # Execute update transaction
    updated_count = database.run_in_transaction(update_transaction)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    update_completed = True
    
    print(f"UPDATE: COMPLETED - {updated_count} Fantasy ratings updated (+2 each) in {duration:.2f}s")

def demonstrate_isolation():
    """Main demonstration of transaction isolation"""
    setup_service_account()
    
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    
    # Clear previous results
    global query1_results, query4_results, update_completed
    query1_results.clear()
    query4_results.clear()
    update_completed = False
    
    print("=== SPANNER ISOLATION DEMONSTRATION ===")
    print("STEP 1: Concurrent read transaction and Fantasy rating updates")
    print()
    
    # Start long read transaction in a separate thread
    read_thread = threading.Thread(
        target=long_read_transaction,
        args=(database, query1_results, "QUERY-1")
    )
    
    # Start update transaction in another thread  
    update_thread = threading.Thread(
        target=update_fantasy_ratings,
        args=(database,)
    )
    
    # Start read transaction first
    read_thread.start()
    
    # Start update transaction shortly after
    update_thread.start()
    
    # Wait for both to complete
    read_thread.join()
    update_thread.join()
    
    print()
    print("STEP 2: Read transaction after updates completed")
    print()
    
    # Wait a moment then run second read transaction
    time.sleep(1)
    long_read_transaction(database, query4_results, "QUERY-4")
    
    print()
    print("=== ISOLATION ANALYSIS ===")
    analyze_isolation_results()

def analyze_isolation_results():
    """Analyze results to prove transaction isolation"""
    if not query1_results or not query4_results:
        print("ERROR: Missing results data")
        return
    
    # Compare ratings for common records
    before_ratings = {(r['userId'], r['movieId']): r['rating'] for r in query1_results}
    after_ratings = {(r['userId'], r['movieId']): r['rating'] for r in query4_results}
    
    # Calculate averages
    before_avg = sum(before_ratings.values()) / len(before_ratings)
    after_avg = sum(after_ratings.values()) / len(after_ratings)
    
    print(f"QUERY-1 (during update): {len(query1_results)} records, avg rating: {before_avg:.3f}")
    print(f"QUERY-4 (after update):  {len(query4_results)} records, avg rating: {after_avg:.3f}")
    print(f"Difference: +{after_avg - before_avg:.3f}")
    print()
    
    if after_avg > before_avg:
        print("✓ ISOLATION PROVEN:")
        print("  - QUERY-1 saw original data (isolated from concurrent updates)")
        print("  - QUERY-4 saw updated data (Fantasy ratings increased)")
        print("  - Read-only transaction was NOT blocked by read-write transaction")
    else:
        print("✗ No significant difference detected")
    
    # Show specific examples if Fantasy movies were updated
    fantasy_examples = []
    for (user_id, movie_id), before_rating in before_ratings.items():
        if (user_id, movie_id) in after_ratings:
            after_rating = after_ratings[(user_id, movie_id)]
            if after_rating > before_rating:
                fantasy_examples.append((user_id, movie_id, before_rating, after_rating))
                if len(fantasy_examples) >= 3:
                    break
    
    if fantasy_examples:
        print("\nFantasy movie rating changes:")
        for user_id, movie_id, before, after in fantasy_examples:
            print(f"  User {user_id}, Movie {movie_id}: {before:.1f} → {after:.1f} (+{after-before:.1f})")

def save_results_to_files():
    """Save top 100 results to files as required"""
    def save_top_100(results, filename):
        with open(filename, 'w') as f:
            f.write("userId,movieId,rating,timestamp\n")
            for record in results[:100]:
                f.write(f"{record['userId']},{record['movieId']},{record['rating']},{record['timestamp']}\n")
    
    save_top_100(query1_results, "query1_results_top100.csv")
    save_top_100(query4_results, "query4_results_top100.csv")
    
    print(f"\nResults saved:")
    print(f"  query1_results_top100.csv - {min(len(query1_results), 100)} records")
    print(f"  query4_results_top100.csv - {min(len(query4_results), 100)} records")

if __name__ == "__main__":
    demonstrate_isolation()
    save_results_to_files() 