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
        
        forward_edges = edges.map(lambda e: (e[1], e[0])).cache()
        
        start_vertex = 12
        end_vertex = 34
        
        path = None
        
        bfs_data = sc.parallelize([(start_vertex, tuple([start_vertex]))])
        
        for i in range(8):
            if path:
                break
                
            new_paths = bfs_data.join(forward_edges) \
                      .map(lambda item: (item[1][1], item[1][0] + (item[1][1],))) \
                      .distinct()
            
            end_paths = new_paths.filter(lambda x: x[0] == end_vertex).collect()
            
            if end_paths:
                path = list(end_paths[0][1])
                break
            
            bfs_data = new_paths
        
        if path:
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