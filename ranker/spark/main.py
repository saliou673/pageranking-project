"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx
Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
import re
import sys
from operator import add
from typing import DefaultDict

from pyspark.sql import SparkSession

# Format input data.
def parse(line):
    res = line.split('\t1\t')
    urls = res[1]\
            .replace('{','')\
            .replace('}','')\
            .replace('(','')\
            .replace(')','')\
            .replace(' ','')\
            .split(',')
    mappedUrl = []
    for url in urls:
        mappedUrl.append(res[0] + ' ' + url)
    return mappedUrl

# Compute the contribution by urls and last rank
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Parses a urls pair string into urls pair.
def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
    base_path = 'gs://dataset-pagerank'
    input_file = base_path + '/data.txt'
    # Loads in input file. It should be in format of:
    lines = spark.read.text(input_file).rdd.map(lambda r:parse(r[0])).flatMap(lambda x:x)

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to the 
    print("=========== R e s u l t a t   d u  r a n k i n g ===========")
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))
    spark.stop()