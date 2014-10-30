'''
Created on Sep 17, 2014

@author: Amit
'''

from utils.ahocorasick import *
import os,sys,json
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
data = []
with open(BASE_DIR + "/../data/relevant_concepts.csv") as fp:
  data = fp.readlines()

list_keywords = []

for line in data:
    list_keywords.append(line.strip().lower())

tagger = getTagger(list_keywords)

for row in sys.stdin:
    try:
        tweet = json.loads(row)
        matches = tagger.tag(tweet['text'].lower())
        if len(matches) > 0:
            print row.strip()
    except:
        pass 
