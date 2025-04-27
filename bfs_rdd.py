from pyspark import SparkContext, SparkConf
import time
import sys

def parse_edge(s):
    """Parse an edge from a text line"""
    user, follower = s.split("\t")
    return (int(user), int(follower))

def step(item):
    """Process a join result to create a new candidate vertex with path"""
    vertex, data = item
    current_data, next_vertex = data
    distance, path = current_data
    return (next_vertex, (distance + 1, path + [next_vertex]))

def main():
    # Initialize Spark
    conf = SparkConf().setAppName("BFS_RDD").setMaster("yarn")
    sc = SparkContext(conf=conf)

    # Start measuring CPU time
    start_time = time.process_time()

    # Load the edges
    edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge)
    
    # For BFS we need edges in the form (to_vertex, from_vertex) to find neighbors
    forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(400).persist()

    # Starting parameters
    start_vertex = 12
    end_vertex = 34
    
    # Initialize distances with starting vertex
    # Format: (vertex, (distance, path))
    distances = sc.parallelize([(start_vertex, (0, [start_vertex]))]).partitionBy(400)
    
    # BFS algorithm
    found = False
    current_distance = 0
    
    while not found:
        # Find neighbors of current frontier
        candidates = distances.join(forward_edges).map(step)
        
        # Update distance map with new candidates
        combined = distances.fullOuterJoin(candidates)
        
        # Keep shortest path for each vertex
        new_distances = combined.map(
            lambda item: (
                item[0], 
                min(
                    [v for v in item[1] if v is not None],
                    key=lambda v: v[0] if v is not None else float('inf')
                )
            )
        ).persist()
        
        # Check if we found the target
        result = new_distances.filter(lambda x: x[0] == end_vertex).collect()
        
        if result:
            # Found a path
            _, (_, path) = result[0]
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
            with open("cpu_time_rdd.txt", "w") as f:
                f.write(str(cpu_time))
                
            found = True
        else:
            # Continue BFS
            current_distance += 1
            distances = new_distances
            
            # Optional: stop after a reasonable number of steps to avoid infinite loops
            if current_distance > 10:  # arbitrary cutoff
                print("Path not found within 10 steps")
                break
    
    sc.stop()

if __name__ == "__main__":
    main() 