#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time

from pyspark.sql import SparkSession, functions as F, types as T

SRC_ID = 12
DST_ID = 34
FILE_PATH = "/data/twitter/twitter_sample.txt"


def main():
    spark = (
        SparkSession.builder
        .appName("twitter-bfs-df")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )

    edges_schema = T.StructType([
        T.StructField("dst", T.IntegerType(), False),
        T.StructField("src", T.IntegerType(), False),
    ])

    edges = (
        spark.read.text(FILE_PATH)
        .select(
            F.split("value", "\t").getItem(0).cast("int").alias("dst"),
            F.split("value", "\t").getItem(1).cast("int").alias("src"),
        )
        .dropna()
        .filter((F.col("src").isNotNull()) & (F.col("dst").isNotNull()))
        .cache()
    )

    edges = edges.repartition("src").persist()

    path_schema = T.StructType([
        T.StructField("vertex", T.IntegerType(), False),
        T.StructField("path", T.ArrayType(T.IntegerType()), False),
    ])

    frontier = spark.createDataFrame([(SRC_ID, [SRC_ID])], schema=path_schema)
    visited = frontier.select("vertex")

    cpu_start = time.time()

    step = 0
    found_path = None

    while True:
        neighbors = (
            frontier.alias("f")
            .join(edges.alias("e"), F.col("f.vertex") == F.col("e.src"), "inner")
            .select(
                F.col("e.dst").alias("vertex"),
                F.expr("array_union(f.path, array(e.dst))").alias("path"),
            )
        )

        neighbors = neighbors.join(visited, "vertex", "left_anti").persist()

        hit = neighbors.filter(F.col("vertex") == DST_ID)
        if hit.count():
            found_path = hit.select("path").first()[0]
            break

        if neighbors.rdd.isEmpty():
            break

        visited = visited.union(neighbors.select("vertex")).distinct().persist()
        frontier = neighbors
        step += 1

    cpu_end = time.time()

    if found_path:
        print(",".join(str(v) for v in found_path))
    else:
        print("No path between {} and {}".format(SRC_ID, DST_ID))

    print("CPU time (DF): {:.3f} s".format(cpu_end - cpu_start))

    spark.stop()


if __name__ == "__main__":
    main()