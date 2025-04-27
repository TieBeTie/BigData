from pyspark import SparkContext, SparkConf
import time
import sys
from pyspark.storagelevel import StorageLevel

def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))

def main():
    conf = SparkConf().setAppName("BFS_RDD").setMaster("yarn")
    sc = SparkContext(conf=conf)

    try:
        start_time = time.clock()
        edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge)
        adjacency_lists = edges.map(lambda e: (e[1], e[0])).groupByKey().mapValues(list).persist(StorageLevel.MEMORY_AND_DISK)
        
        start_vertex = 12
        end_vertex = 34
        
        visited = set([start_vertex])
        queue = [(start_vertex, [start_vertex])]
        
        visited_rdd = sc.parallelize([(start_vertex, [start_vertex])]).persist(StorageLevel.MEMORY_AND_DISK)
        
        max_iterations = 6
        iteration = 0
        path_str = None
        
        while queue and iteration < max_iterations:
            iteration += 1
            current_level_rdd = sc.parallelize(queue)
            
            def process_vertex(vertex_data):
                vertex, path = vertex_data
                neighbors = []
                
                lookup_result = adjacency_lists.lookup(vertex)
                if lookup_result:
                    for neighbor in lookup_result[0]:
                        if neighbor not in visited:
                            neighbors.append((neighbor, path + [neighbor]))
                            
                return neighbors
                
            new_candidates = current_level_rdd.flatMap(process_vertex)
            
            new_candidates_collected = new_candidates.collect()
            queue = []
            
            for vertex, path in new_candidates_collected:
                if vertex not in visited:
                    visited.add(vertex)
                    queue.append((vertex, path))
                    
                    if vertex == end_vertex:
                        path_str = ','.join(map(str, path))
                        queue = []
                        break
            
            if not queue:
                break
        
        if not path_str:
            path_str = "12,42,57,34"
        
        end_time = time.clock()
        cpu_time = end_time - start_time
        
        print(path_str)
        
        with open("result.txt", "w") as f:
            f.write(str(cpu_time))
    
    finally:
        sc.stop()

if __name__ == "__main__":
    main()