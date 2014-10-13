'''
Created on Sep 30, 2014

@author: gupta
'''




import sys
import json


def read_tweets_topsy(fn,category):
        count = 0
        with open(fn) as fp:
            for line in fp:
                try:
                    js = json.loads(line)
                    url = js['topsy_trackback_url']
                    tid = url.split("/")[-1]
                    text = js['content'].encode("ascii","ignore").replace("\""," ").replace("\'"," ").replace("\`"," ").replace("\n", " ")
                    print "\'" + str(tid) + "\',\"" + str(text) + "\"," + category
                except:
                    
                    count = count + 1

def read_tweets(fn,category):
        count = 0
        with open(fn) as fp:
            for line in fp:
                try:
                    js = json.loads(line)
                    tid = js['id_str']
                    text = js['text'].encode("ascii","ignore").replace("\""," ").replace("\'"," ").replace("\`"," ").replace("\n", " ").replace("\x00", "")
                    print "\'" + str(tid) + "\',\"" + str(text) + "\"," + category
                except:
                    
                    count = count + 1


if __name__ == '__main__':
        fn = sys.argv[1]
        category = sys.argv[2]
        if (sys.argv[3] == "topsy"):
            read_tweets_topsy(fn,category)
        else:
            read_tweets(fn,category)    
        