#!/usr/bin/python
from org.apache.pig.scripting import *
import sys
import os

pigQuery = Pig.compile("""
lastComputedPagerank = 
    LOAD '$inputData' 
    USING PigStorage('\t') 
    AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );

pagerankContribution = 
    FOREACH lastComputedPagerank 
    GENERATE 
        pagerank / COUNT ( links ) AS contrib, 
        FLATTEN ( links ) AS to_url; 

newPageRanking = 
    FOREACH 
        ( COGROUP pagerankContribution BY to_url, lastComputedPagerank BY url INNER )
    GENERATE 
        group AS url, 
        ( 1 - $d ) + $d * SUM (pagerankContribution.contrib) AS pagerank, 
        FLATTEN ( lastComputedPagerank.links ) AS links, SUM (pagerankContribution.contrib) as contrib;

STORE newPageRanking 
    INTO '$outputData' 
    USING PigStorage('\t');
""")

params = {
    'd': '0.85',
    'inputData': 'gs://dataset-pagerank/data.txt',
    'outputData': ''
    }

outputDir = "gs://dataset-pagerank"
    
TOTAL_ITERATION  = 10
for i in range(TOTAL_ITERATION):
	output = outputDir + "/rankpage-iter-" + str(i + 1)
	params["outputData"] = output
	Pig.fs("rmr " + output)
	stats = pigQuery.bind(params).runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	params["inputData"] = output



