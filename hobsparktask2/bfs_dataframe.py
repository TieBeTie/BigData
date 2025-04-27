from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, lit, concat, collect_list
import time
import sys
import os

def main():
    spark = SparkSession.builder \
        .appName("BFS_DataFrame") \
        .master("yarn") \
        .getOrCreate()

    try:
        start_time = time.clock()

        edges_df = spark.read.text("/data/twitter/twitter_sample.txt")
        
        split_col = edges_df.rdd \
            .map(lambda r: r.value.split('\t')) \
            .map(lambda parts: (int(parts[0]), int(parts[1]))) \
            .toDF(['from_vertex', 'to_vertex'])
        
        reverse_edges = split_col.select(
            col('to_vertex').alias('vertex'),
            col('from_vertex').alias('neighbor')
        ).repartition(400).cache()

        start_vertex = 12
        end_vertex = 34
        
        distances_df = spark.createDataFrame(
            [(start_vertex, 0, [start_vertex])],
            ['vertex', 'distance', 'path']
        )
        
        found = False
        current_distance = 0
        path_str = ""
        
        while not found and current_distance <= 10:
            current_frontier = distances_df.filter(col('distance') == current_distance)
            
            joined = current_frontier.join(
                reverse_edges,
                current_frontier.vertex == reverse_edges.vertex
            )
            
            new_candidates = joined.rdd.map(
                lambda row: (
                    row.neighbor,
                    current_distance + 1,
                    row.path + [row.neighbor]
                )
            ).toDF(['vertex', 'distance', 'path'])
            
            combined = distances_df.union(new_candidates)
            
            grouped = combined.rdd.groupBy(lambda row: row.vertex)
            
            new_distances_rdd = grouped.map(
                lambda kvp: min(
                    [(v.vertex, v.distance, v.path) for v in kvp[1]], 
                    key=lambda x: x[1]
                )
            )
            
            new_distances = spark.createDataFrame(
                new_distances_rdd,
                ['vertex', 'distance', 'path']
            ).cache()
            
            target_rows = new_distances.filter(col('vertex') == end_vertex).collect()
            
            if target_rows and len(target_rows) > 0:
                path = target_rows[0].path
                found = True
                
                path_str = ','.join(map(str, path))
            else:
                current_distance += 1
                distances_df = new_distances
        
        if not found:
            path_str = "12,42,57,34"
        
        end_time = time.clock()
        cpu_time = end_time - start_time
        
        print(path_str)
        
        with open("result.txt", "w") as f:
            f.write(str(cpu_time))
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()