'''
Created on Oct 14, 2014

@author: gupta
'''

import json
import sys
from utils import *
import numpy
datadir = "/Users/gupta/Documents/workspace/find_concepts/final_dataset/tweets/" 


def estimate_frequency_single(filename):
    tweets_time = []
    prev_time = -1
    count = 0
    total_count = 0
    ans = []
    with open(filename) as fp:
        for line in fp:
            total_count += 1
            tw = json.loads(line.strip())
            cur_time = int(tw['trackback_date'])
            if prev_time == -1 or cur_time < prev_time:
                prev_time = cur_time
            else:
                ans.append((cur_time - prev_time))
                prev_time = cur_time
    
    #print (sorted(ans))
    if not ans or total_count < 200:
        return 0,0
    else:            
        return (sum(ans)/len(ans),numpy.median(numpy.array(ans))) 

def estimate_frequency(filename):
    tweets_time = []
    prev_time = -1
    count = 0
    ans = []
    with open(filename) as fp:
        for line in fp:
            tw = json.loads(line.strip())
            cur_time = int(tw['trackback_date'])
            if prev_time == -1:
                prev_time = cur_time
                count = 1
            elif count == 10:
                ans.append((cur_time - prev_time)*1.0/10)
                count = 0
                prev_time = -1
            else:
                count += 1    
    ans = sorted(ans)
    print ans
    
    if not ans:
        return 0,0
    else:            
        return (sum(ans)/len(ans),numpy.median(numpy.array(ans)))            
     
# stoplist = []
# 
# with open("stoplists") as fp:
#     for line in fp:
#         stoplist.append(line.strip().lower())

def difference_files(file1,file2):
    sub = {}
    with open(file2) as fp:
        for line in fp:
            sub[line.strip().lower().replace(" ","_")] = 1
    with open(file1) as fp:
        for line in fp:
            token1 = line.strip()
            if token1 not in sub:# and token1 not in stoplist and len(token1) > 5:
                print token1    
  
def remove_nodes_graph(graphfile,nodefile):  
    sub = {}
    with open(nodefile) as fp:
        for line in fp:
            sub[line.strip().lower().replace(" ","_")] = 1
    
    with open(graphfile) as fp:
        for line in fp:
            tokens = line.split()
            if not tokens[0] in sub and not tokens[1] in sub:
                print line.strip()
        
def compute_estimate_freq(concept):
    fn_regex = datadir +  concept + '_?0*.txt_clean'
    list_files = get_list_files(fn_regex)

if __name__ == '__main__':
    #remove_nodes_graph(sys.argv[1], sys.argv[2])
    #difference_files(sys.argv[1], sys.argv[2])
    
    avg,median = estimate_frequency_single(sys.argv[1])
    #if median == 0:
    #    print 0
    #else:    
    #    print str(60*60*24/median)
    #if x > 0:
    #    keyword = "_".join(sys.argv[1].split("_")[0:-1])
    #    print keyword + "," + str(x)
    #tweets_time = estimate_frequency(sys.argv[1])
    #print sys.argv[1]
    #for x in tweets_time:
    #    print x
    
    