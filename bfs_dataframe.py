from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit, concat_ws
import time
import sys

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("BFS_DataFrame") \
        .master("yarn") \
        .getOrCreate()

    # Start measuring CPU time
    start_time = time.process_time()

    # Load graph edges
    edges_df = spark.read.text("/data/twitter/twitter_sample.txt") \
        .repartition(400)

    # Parse the edges
    split_col = spark.read.text("/data/twitter/twitter_sample.txt").rdd \
        .map(lambda r: r.value.split('\t')) \
        .toDF(['from_vertex', 'to_vertex'])
    
    # Convert string columns to integers
    edges_df = split_col \
        .select(
            col('from_vertex').cast('int'), 
            col('to_vertex').cast('int')
        )
    
    # Create reverse edges for finding neighbors
    reverse_edges = edges_df.select(
        col('to_vertex').alias('vertex'),
        col('from_vertex').alias('neighbor')
    )
    
    # Cache the edges for performance
    reverse_edges.cache()

    # Define starting parameters
    start_vertex = 12
    end_vertex = 34
    
    # Initialize distances dataframe with starting vertex
    # Schema: vertex, distance, path (as array of vertices)
    distances_df = spark.createDataFrame(
        [(start_vertex, 0, [start_vertex])],
        ['vertex', 'distance', 'path']
    )
    
    # BFS algorithm
    found = False
    current_distance = 0
    
    while not found:
        # Find neighbors of the current frontier
        current_frontier = distances_df.filter(col('distance') == current_distance)
        
        # Join with edges to get neighbors
        new_candidates = current_frontier.join(
            reverse_edges,
            current_frontier.vertex == reverse_edges.vertex
        ).select(
            reverse_edges.neighbor.alias('vertex'),
            (current_distance + 1).alias('distance'),
            concat_ws(',', 
                array([col(f'path[{i}]') for i in range(current_frontier.schema['path'].dataType.elementType.length())]),
                reverse_edges.neighbor
            ).alias('path_str')
        )
        
        # Convert path_str back to array for further processing
        new_candidates = new_candidates.withColumn(
            'path', 
            array([col(f'path_str').cast('int')])
        ).drop('path_str')
        
        # Combine with existing distances
        all_distances = distances_df.union(new_candidates)
        
        # Get shortest distance for each vertex
        distances_df = all_distances.groupBy('vertex').agg(
            {'distance': 'min', 'path': 'first'}
        ).select(
            col('vertex'),
            col('min(distance)').alias('distance'),
            col('first(path)').alias('path')
        )
        
        # Check if we've found the target vertex
        target_row = distances_df.filter(col('vertex') == end_vertex).collect()
        
        if target_row and len(target_row) > 0:
            path = target_row[0]['path']
            # Stop measuring CPU time
            end_time = time.process_time()
            # Output the path
            path_str = ','.join(map(str, path))
            print(path_str)
            
            # Save the path to result.txt for testing
            with open("result.txt", "w") as f:
                f.write(path_str)
            
            # Print the CPU time
            cpu_time = end_time - start_time
            print(f"CPU time: {cpu_time} seconds")
            
            # Save the time measurement for submission
            with open("cpu_time_df.txt", "w") as f:
                f.write(str(cpu_time))
                
            found = True
        else:
            # Continue BFS
            current_distance += 1
            
            # Optional: stop after reasonable number of steps
            if current_distance > 10:  # arbitrary cutoff
                print("Path not found within 10 steps")
                break
    
    spark.stop()

if __name__ == "__main__":
    main() 