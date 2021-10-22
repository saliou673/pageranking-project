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

    # Initialisation de spark.
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
    
    base_path = 'gs://dataset-pagerank'
    input_file = base_path + '/data.txt'
    # Chargement des données:
    lines = spark.read.text(input_file).rdd.map(lambda r:parse(r[0])).flatMap(lambda x:x)

    # Chargement des urls et initialisation de leurs voisins.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Chargement de toutes les URLs vers lesquelles d'autres URL sont liées
    # à partir du fichier d'entrée et initialise leur rang à 1.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calcul itératif des rangs.
    for iteration in range(10):
        # Calcul de la contribution par URL.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    print("=========== R e s u l t a t   d u  r a n k i n g ===========")
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))
    spark.stop()