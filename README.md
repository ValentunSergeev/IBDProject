# IBDProject

## Problem Statement
We will implement a search engine that finds the relevance of each document in a relatively large dataset to an input of a search query.
This problem will be tackled using Hadoop's MapReduce where the text corpus is too large to fit in one machine. The main task of the search engine can be divided into two main parts:
- Document indexnig
- Query answering

The document indexing will produce a structured form of data that is easy to interpret and deal with for query processing.
The query answering job will take this structured data and produce the output of the search query.

## Vector space model with mapreduce

As discussed earlier. To implement the search engine, we need to perform two main tasks, Indexing and Query processing.
For implementing these tasks in Haoop's MapReduce, we have three MapReduce Jobs:

- DocumentCount
- IndexerJob
- RelevanceAnalizator

The schema of our implementation is described in the diagram below.


![](https://i.imgur.com/ZgB22Vt.png)

