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

  doc_to_line = text.map(lambda x: (x.split(" ")[0], (x.split(" ")[1:])))
  for y in doc_to_line.collect():
      print(y)
  
  pairs = doc_to_line.flatMap(lambda kv: ((kv[0], x) for x in (kv[1])))
  for z in pairs.collect():
      print(z)

  individual_occurence = pairs.map(lambda kv: (kv[0], (kv[1],1)))
  for a in individual_occurence.collect():
      print(a)
  
