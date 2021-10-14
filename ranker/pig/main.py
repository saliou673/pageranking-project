#!/usr/bin/python
from org.apache.pig.scripting import *

pigQuery = Pig.compile("""
lastComputedPagerank = 
    LOAD '$inputData' 
    USING PigStorage('\t') 
    AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );

pagerankContribution = 
    FOREACH lastComputedPagerank 
    GENERATE 
        pagerank / COUNT ( links ) AS pagerank, 
        FLATTEN ( links ) AS to_url; 

newPageRanking = 
    FOREACH 
        ( COGROUP pagerankContribution BY to_url, lastComputedPagerank BY url INNER )
    GENERATE 
        group AS url, 
        ( 1 - $d ) + $d * SUM ( pagerankContribution.pagerank ) AS pagerank, 
        FLATTEN ( lastComputedPagerank.links ) AS links;
        
STORE newPageRanking 
    INTO '$outputData' 
    USING PigStorage('\t');
""")

params = {
    'd': '0.5',
    'inputData':
    '../../data/data.txt',
    'outputData': ''
    }
outputDir = "target"
TOTAL_ITERATION  = 10
for i in range(TOTAL_ITERATION):
	output = outputDir + "/rankpage-iter-" + str(i + 1)
	params["outputData"] = output
	Pig.fs("rmr " + output)
	stats = pigQuery.bind(params).runSingle()
	if not stats.isSuccessful():
		raise 'failed'
	params["inputData"] = output



