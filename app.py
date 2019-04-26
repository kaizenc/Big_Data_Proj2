#!/usr/local/bin/python3
"""SimpleApp.py"""
import os
import time
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

from math import log10, sqrt
import itertools

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'


def similarity(arr1, arr2):
    numerator = 0
    sqrt1 = 0
    sqrt2 = 0
    for i, _ in enumerate(arr1):
        if arr1[i] != arr2[i] and arr1[i]*arr2[i] != 0:
            for i, _ in enumerate(arr1):
                numerator += (arr1[i] * arr2[i])
                sqrt1 += arr1[i]**2
                sqrt2 += arr2[i]**2
            denominator = sqrt(sqrt1) * sqrt(sqrt2)
            if denominator == 0:
                return 0
            return numerator / denominator
    return 0


def vectorize(total, arr):
    res = [0 for i in range(total)]
    for x in arr:
        res[x[0] - 1] = x[1]
    return res

def generate_pairs(kv):
    pairs = []
    words = kv[1]
    for i, x in enumerate(words):
        for j in range(i+1, len(words)):
            pairs.append((words[i], words[j]))
    return pairs

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Big Data Project 2")
    sc = SparkContext(conf=conf)

    # collect textfile, turn it into an RDD
    text = sc.textFile('test_file.txt')
    totalDocs = text.count()
    # split up the words per doc
    doc_to_line = text.map(lambda x: (x.split(" ")[0], (x.split(" ")[1:])))
    # add document word count to each document
    # Tuple: ( (doc1, words_in_doc), [word1, word2, word3, ...] )
    doc_to_line_word_count = doc_to_line.map(lambda kv: ((kv[0], len(kv[1])), kv[1]))

    # create doc/word tuples, including the document word count and a "1" to serve as a word count
    # Tuple: ( (doc1, words_in_doc, word1), 1 )
    doc_word_pairs_init = doc_to_line_word_count.flatMap(lambda kv: (((kv[0], x), 1) for x in (kv[1])))
    # Reduce by adding the 1s to get the number of appearances for a word per document
    # Tuple: ( (doc1, words_in_doc, word1), word_frequency )
    doc_word_pairs = doc_word_pairs_init.reduceByKey(lambda x, y: x + y)
    # Re-map for clarity and to calculate tf
    # Tuple: ( doc1, (word1, words_in_doc, word_frequency) )
    pairs_with_tf_init = doc_word_pairs.map(lambda kv: (kv[0][0][0], (kv[0][1], kv[1], kv[0][0][1])))
    # Calculate tf
    # Tuple: ( doc1, (word1, words_in_doc, word_frequency, tf) )
    pairs_with_tf = pairs_with_tf_init.map(lambda x: (x[0], x[1] + ((x[1][1] / x[1][2]),)))

    # Re-map to prepare for idf calculation, also turns the value into an array for reduction
    # Tuple: ( word1, [(doc1, words_in_doc, word_frequency, tf)] )
    idf_init = pairs_with_tf.map(lambda x: (x[1][0], [(x[0],) + x[1][1:]]))
    # Reduce by concatenating all the arrays
    # Tuple: ( word1, [(doc1 ... ), (doc2 ... )] )
    idf_init_2 = idf_init.reduceByKey(lambda x, y: x + y)
    # Calculate idf
    # Tuple: ( (word1, idf), [(doc1 ... ), (doc2 ... )] )
    pairs_with_idf = idf_init_2.map(lambda x: ((x[0], (log10(totalDocs / len(x[1])))), x[1]))

    # Calculate tf*idf for each word/doc pair
    # Also strip the characters "doc" from each appearance and convert it to an int
    # Tuple: ( word1, [(1, tf*idf), (2, tf*idf)] )
    tf_idf = pairs_with_idf.map(lambda kv: (kv[0][0], [(int(x[0][3:]), x[3] * kv[0][1]) for x in kv[1]]))
    # Convert each kv pair into a (word, vector) pair
    # Tuple: ( word1, [x, y, z, ... ] )
    # Note: Each x, y, z corresponds to a tf*idf value for that word per document
    vecotrized = tf_idf.map(lambda x: (x[0], vectorize(totalDocs, x[1]))).sortByKey()

    rdd3 = rdd2.map(lambda x: (0, [x])).reduceByKey(lambda x, y: x + y).flatMap(generate_pairs)\
        .map(lambda x: ( similarity(x[0][1], x[1][1]), (x[0][0], x[1][0]) )).sortByKey(ascending=False)
