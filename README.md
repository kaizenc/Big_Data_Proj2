# Big_Data_Proj_2
Project 2 for Big Data; CS493 Hunter College

## What is happening?

### Problem
• Given m documents, compute the term-term relevance using
MapReduce and Spark

• Input: A text file, each line represents a document

• Output: A list of term-term pairs sorted by their similarity descending
t1 t2 s1 t3 t4 s2


### Sub-problems:
• Compute Term Frequency – Inverse Document Frequency (TF-IDF)
for each term

• Output: mxn matrix (m: #documents, n: #terms)

• Computer and sort term-term relevance between a query term and all terms associated with the TF-IDF matrix

• Input: a query term t

• Output: term-term relevance between the query term and those terms in the tfidf matrix sorted by the relevance score (descending)

