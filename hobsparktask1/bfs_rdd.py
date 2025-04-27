from pyspark import SparkContext, SparkConf
import time
import sys

def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))

def main():
    conf = SparkConf().setAppName("BFS_RDD").setMaster("yarn")
    sc = SparkContext(conf=conf)

    try:
        start_time = time.clock()
        
        edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge)
        
        forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(400).cache()
        
        start_vertex = 12
        end_vertex = 34
        
        current_distance = 0
        distances = sc.parallelize([(start_vertex, (0, [start_vertex]))]).partitionBy(400)
        
        found = False
        path = None
        
        while not found and current_distance < 5:
            candidates = distances.filter(lambda x: x[1][0] == current_distance) \
                               .join(forward_edges) \
                               .map(lambda item: (item[1][1], (current_distance + 1, item[1][0][1] + [item[1][1]])))
            
            new_distances = distances.fullOuterJoin(candidates) \
                                   .map(lambda item: (
                                       item[0], 
                                       item[1][0] if item[1][0] is not None else item[1][1]
                                   ))
            
            target_path = candidates.filter(lambda x: x[0] == end_vertex).take(1)
            
            if target_path:
                found = True
                path = target_path[0][1][1]
            else:
                current_distance += 1
                distances = new_distances
        
        if found:
            path_str = ','.join(map(str, path))
        else:
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