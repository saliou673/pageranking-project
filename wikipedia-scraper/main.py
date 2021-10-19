import json
import requests
import logging as logger
from threading import Thread
from bs4 import BeautifulSoup

LIMIT = 1250*5*2
MAX_ITER = 10
# todo: extract logger config and default ouput path to config file.

logger.basicConfig(level=logger.INFO)

def getUrl(offset=0):
    return """https://wikidata.demo.openlinksw.com/sparql?
                default-graph-uri=http%3A%2F%2Fwww.wikidata.org%2F
                &query=SELECT+%3Fsitelink%0D%0AWHERE+%7B%0D%0A++%3Fitem+wdt%3AP21+wd%3AQ6581097%3B%0D%0A++++++++wdt%3AP31
                +wd%3AQ5.%0D%0A++%3Fsitelink+schema%3Aabout+%3Fitem%3B%0D%0A++schema%3AisPartOf
                +%3Chttps%3A%2F%2Fen.wikipedia.org%2F%3E.%0D%0A%7D+%0D%0A
                LIMIT+""" + str(LIMIT) + """+OFFSET+""" + str(offset) +"""&format=application%2Fsparql-results%2Bjson
                &timeout=0&signal_void=on&signal_unconnected=on"""

def task(filename, offset):
    response = requests.get(getUrl(offset))
    json_data = json.loads(response.text)
    f = open(filename, 'w')
    for item in json_data['results']['bindings']:
        link = item['sitelink']['value']
        logger.info('\t Processing: %s', link)
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.select("#bodyContent a")
        tagLength = len(links)
        isFirstItem = True
        f.write(link + ' 1 {')
        for tag in links:
            tagLength -= 1
            if(not tag.has_attr('href')):
                continue
            value = tag['href']
            if value.startswith('#') or (tag.has_attr('title')  and tag['title'].startswith('Edit')):
                links.remove(tag)
                continue
            if value.startswith('/wiki'):
                value = link.split('/wiki')[0] + value
            if isFirstItem:
                f.write('(' + value +')')
                isFirstItem = False
            else:
                f.write( ', (' + value + ')')
        f.write(' }\n')
    f.close()


counter = 0
offset = 0
threadList = []
while counter < MAX_ITER :
    filename = 'data/data_iter'+ str(counter+1) + '.txt'
    thread = Thread(target=task, args=(filename, offset, ))
    thread.start()
    threadList.append(thread)
    logger.info('Thread %d started...', (counter+1))
    offset += LIMIT
    counter += 1

for thread in threadList:
    thread.join()

    
    