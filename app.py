#!/usr/local/bin/python3
"""SimpleApp.py"""
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Big Data Project 2")
    sc = SparkContext(conf=conf)

    # collect textfile, turn it into an RDD
    text = sc.textFile('test_file.txt')
    # split up the words per doc
    doc_to_line = text.map(lambda x: (x.split(" ")[0], (x.split(" ")[1:])))
    # create doc/word pairs and count 
    pairs = doc_to_line.flatMap(lambda kv: (((kv[0], x), 1) for x in (kv[1])))\
        .reduceByKey(lambda x, y: x+y)
    # remap so that the doc is the key
    count_map = pairs.map(lambda kv: (kv[0][0], [(kv[0][1], kv[1])]))
    # reduce so that each doc now has an array of words and their count
    pre_dataFrame = count_map.reduceByKey(lambda x, y: x+y).sortByKey()
    for x in pre_dataFrame.collect():
        print(x)
