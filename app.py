"""SimpleApp.py"""
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
 
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Print Contents of RDD - Python")
  sc = SparkContext(conf=conf)

  text = sc.textFile('test_file.txt')
  for x in text.collect():
      print(x)

  pairs = text.map(lambda x: (x.split(" ")[0], (x.split(" ")[1:])))
  for y in pairs.collect():
      print(y)