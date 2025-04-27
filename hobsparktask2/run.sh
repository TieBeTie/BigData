#!/bin/bash

# Ensure output file is created with proper permissions
touch result.txt
chmod 777 result.txt

# Run the Spark job
spark2-submit bfs_dataframe.py 