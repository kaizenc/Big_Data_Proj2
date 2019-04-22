#!/usr/local/bin/python3
"""SimpleApp.py"""
import os
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

from math import log10, sqrt

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'


def similarity(arr1, arr2):
    numerator = 0
    sqrt1 = 0
    sqrt2 = 0
    for i in range(len(arr1)):
        numerator += arr1[i] * arr2[i]
        sqrt1 += arr1[i]**2
        sqrt2 += arr2[i]**2
    denominator = sqrt(sqrt1) * sqrt(sqrt2)
    return numerator / denominator

def single_numerator(arr):
    term_numerator = 0
    for i in range(len(arr)):
        term_numerator += arr[i]
    return term_numerator

def single_denominator(arr):
    term_denominator = 0
    for i in range(len(arr)):
        term_denominator += arr[i]**2
    return term_denominator


def vectorize(total, arr):
    res = [0 for i in range(total)]
    for x in arr:
        res[x[0] - 1] = x[1]
    return res


if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Big Data Project 2")
    sc = SparkContext(conf=conf)

    # collect textfile, turn it into an RDD
    text = sc.textFile('testdata.txt')
    totalDocs = text.count()
    # split up the words per doc
    doc_to_line = text.map(lambda x: (x.split(" ")[0], (x.split(" ")[1:])))
    # add document word count to each document
    doc_to_line2 = doc_to_line.map(lambda kv: ((kv[0], len(kv[1])), kv[1]))

    # create doc/word pairs and count
    pairs = doc_to_line2\
        .flatMap(lambda kv: (((kv[0], x), 1) for x in (kv[1])))\
        .reduceByKey(lambda x, y: x + y)
    # at this stage: ((doc, length), word), word_count
    new_pairs = pairs.map(lambda kv: (kv[0][0][0], (kv[0][1], kv[1], kv[0][0][1])))
    pairs_with_tf = new_pairs.map(lambda x: (x[0], x[1] + ((x[1][1] / x[1][2]),)))

    flip = pairs_with_tf.map(lambda x: (x[1][0], [(x[0],) + x[1][1:]]))\
        .reduceByKey(lambda x, y: x + y)
    # Current state: (word, [doc, word_appearance, doc_length, tf])
    pairs_with_idf = flip.map(lambda x: ((x[0], (log10(totalDocs / len(x[1])))), x[1]))

    rdd1 = pairs_with_idf.map(lambda kv: (kv[0][0], [(int(x[0][3:]), x[3] * kv[0][1]) for x in kv[1]]))\
        .map(lambda x: (x[0], vectorize(totalDocs, x[1])))

    similarity_numerator = rdd1.map(lambda x: (x[0], single_numerator(x[1])))

    similarity_denominator = similarity_numerator.map(lambda x: (x[0], sqrt(x[1]**2)))

    for x in similarity_denominator.collect():
        print(x)

    query_term = input("What is your query term? ")
    print(query_term)
    

    # l = list(itertools.combinations(rdd1.toLocalIterator(),2))
    # rdd2 = sc.parallelize(l)
    # final = rdd2.map(lambda x: (similarity(x[0][1], x[1][1], (x[0][0], x[1][0]))))
    # for x, i in enumerate(final.sortByKey(ascending=False).collect()):
    #     print(i)
    #     if x == 10:
    #         break
