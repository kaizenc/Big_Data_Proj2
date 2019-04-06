"""SimpleApp.py"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

file_name = 'test_file.txt'

if __name__ == '__main__':
    # Start reading a file on your system
    cwd = os.getcwd() + '/'
    logFile = cwd + file_name  # Should be some file on your system
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    textFile = spark.read.text(logFile).cache()

    # Count the lines with a's, count the lines with b's
    numAs = textFile.filter(textFile.value.contains('a')).count()
    numBs = textFile.filter(textFile.value.contains('b')).count()
    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

    # Return the size of the line with the most number of words
    maxWords = textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(col("numWords"))).collect()
    print(maxWords)

    # Example of a word count mapreduce flow; count each word, group and sort by count
    wordCounts = textFile.select(explode(split(textFile.value, "\s+")).alias("word")).groupBy("word").count().orderBy("count").collect()
    print(wordCounts)

    spark.stop()