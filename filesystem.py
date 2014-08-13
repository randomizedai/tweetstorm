'''
Created on Aug 6, 2014

@author: Amit
'''

import os
import json
from util import *

def make_dir(path):
    try:
        os.makedirs(path)
    except Exception, e:
        if e.strerror != 'File exists':
            return "exception"
            

## returns file size in MB
def filesize(path):
    if os.path.isfile(path):
        return os.path.getsize(path) * 1 / ( 1024 * 1024)
    
    return 0

def create_new_file(path,name):
    make_dir(path)
    f = open(path + "/" + name,"w")
    return f

def get_file_handle(path):
    make_dir(os.path.dirname(path))
    f = open(path,"a")
    return f
    
    
def dump_tweets_into_file(file_handle,tweets):
    for tw in tweets:
        file_handle.write(json.dumps(tw) + "\n")
    
    file_handle.flush()    
    
if __name__ == '__main__':
    pass