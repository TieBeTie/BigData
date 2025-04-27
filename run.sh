#!/bin/bash

# Ensure output files are created with proper permissions
touch result.txt
chmod 777 result.txt

# Run the Spark job
spark2-submit bfs_rdd.py

# Display the results
echo "BFS Result:"
cat result.txt
echo "CPU Time (seconds):"
cat cpu_time_rdd.txt 