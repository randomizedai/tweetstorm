'''
Created on Nov 13, 2014

@author: gupta
'''
'''
Created on Aug 6, 2014

@author: Amit

Utility Functions
'''
import ConfigParser
from functools import wraps
import datetime
import time
import socket
import ntpath
import uuid
import os
import sys
import traceback
import re
import requests
import timeout_decorator
import json
from nltk.stem.wordnet import WordNetLemmatizer
from functools import wraps



punctuations = ["/","(",")","\\","|", ":",",",";",".","?", "!"]
quotes = ["\"","\\","\/"]
clean = re.compile('^[a-z \-]+$')
lmtzr = WordNetLemmatizer()

def memo(func):
    cache = {}
    @wraps(func)
    def wrap(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]
    return wrap

@memo
def stemw(word):
    return lmtzr.lemmatize(word)


def read_file(filename):
    with open(filename) as fp:
        lines = fp.readlines()
        
    lines = [x.strip() for x in lines]
    return lines    

def read_json_file(filename):
    lines = read_file(filename)
    ans = []
    for line in lines:
        try:
            js = json.loads(line)
            ans.append(js)
        except:
            pass
    return ans             





def check_file_exists(fn):
    return os.path.isfile(fn)

def datetimevalue(num):
    return str(datetime.datetime.fromtimestamp(int(num)).strftime('%Y-%m-%d %H:%M:%S'))


@timeout_decorator.timeout(30)
def get_request(query):
    r = requests.get(query)
    return r               


@memo 
def stem_string(sentence):
    tokens = sentence.split()
    stemmed_tokens = [stemw(token) for token in tokens]
    return " ".join(stemmed_tokens)

def print_exec_error(worker_id):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print("********#####EXCEPTION#####********* taskid--"+ str(worker_id),exc_type, fname, exc_tb.tb_lineno)
   
        

@memo
def alpha_string(strv):
    ltoken = strv.lower()
    if clean.match(ltoken):
        return True
    return False 

def get_absolute_path(relative_path):
    dir = os.path.dirname(__file__)
    filename = os.path.join(dir, relative_path)
    return filename


def get_random_uuid():
    s = str(uuid.uuid4())
    s = s.replace("-","_")
    return s


def get_host_name():
    return socket.gethostbyaddr(socket.gethostname())[0]

def value_string(val):
    if type(val) == str:
        return "\'" + val + "\'"
    return str(val)

def create_assignment_string(dict_values, delim = ","):
    list_values = []
    
    for k,v in dict_values.items():
        list_values.append(k + "=" + value_string(v))
    
    return delim.join(list_values)


def get_timestamp_from_time(ts,format):
    st = datetime.datetime.fromtimestamp(ts).strftime(format)
    return st

def get_current_timestamp():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    return st

def get_current_date():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d')
    return st

def get_current_time():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%H%M%S')
    return st


def get_current_hour():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%H')
    return st

def bool_string(desc,value):
    if value:
        return str(desc) + str(value)
    else:
        return ""  


def compute_max_id(tweets):
    ans = 0
    if tweets:
        ans = max([long(x['id_str']) for x in tweets])
    return str(ans)

def compute_min_id(tweets):
    ans = 0
    if tweets:
        ans = min([long(x['id_str']) for x in tweets])
    return str(ans)

def compute_max_time(tweets):
    ans = 0
    if tweets:
        ans = max([x['created_at'] for x in tweets])
    return ans

def compute_min_time(tweets):
    ans = 0
    if tweets:
        ans = min([x['created_at'] for x in tweets])
    return ans



def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def get_dict_from_twitterobject(to):
    ans = {}
    for k,v in to.items():
        ans[k] = v
    return ans



def clean_tweet(tweet_text):
    tweet_text = tweet_text.strip()
    
    
    tweet_text = tweet_text.replace("\\n"," ").replace("\n", " ").replace("-"," ")
 
    tokens = tweet_text.split()
    new_tokens = []
    
    for token in tokens:
        
        if  token == "RT"  or token.startswith("@")   or token.startswith("/") or token.endswith("..."):
            continue
        #elif token.startswith("#"):
        #    new_token = new_token[1:]
        elif token.startswith("http"):
            new_token = "."
        elif token.startswith("&amp"):
            new_token = "and"    
        elif token.startswith("&"):
            continue
        else:
            new_token = token    
        new_tokens.append(new_token)
    
    tweet_text = " ".join(new_tokens)     
    for x in punctuations:
        tweet_text = tweet_text.replace(x , " " + x + " ")
       
    
    tokens = tweet_text.split()
    prev_punct = False
    new_tokens = []
    for i,token in enumerate(tokens):
        new_token = ""
        is_punct = (token in punctuations)
        if (prev_punct and is_punct) or (is_punct and i == 0):
            continue
        else:
            new_token = token    
        new_tokens.append(new_token)
        prev_punct = is_punct  
          
    new_text = " ".join(new_tokens)
    if not new_tokens:
        return  "NULL_TWEET"      
    if  not new_text.endswith("."):
        new_text += " ."
   
    for quote in quotes:
        new_text = new_text.replace(quote," ")
    new_text = "".join([c for c in new_text if ord(c) < 128])
    new_text = " ".join(new_text.split())
    return new_text  


def create_clean_tweets_file(filename,clean_filename):
    tweets = []
    with open(filename) as fp:
        for line in fp:
            try:
                tw = json.loads(line.strip())
                tweets.append(tw)
            except:
                pass    
             
    with open(clean_filename,"w") as fp:
        for tw in tweets:
            text = tw['content']
            clean_text = clean_tweet(text)
            fp.write(clean_text + "\n")



def get_dict_from_twitterobject_recursive(to):
    if isinstance(to,TwitterObject) or isinstance(to,dict):
        ans = {}
        for k,v in to.items():
            ans[k] = get_dict_from_twitterobject_recursive(v)
        return ans    
    elif isinstance(to,list):
        return [ get_dict_from_twitterobject_recursive(x) for x in to ]
    else:
        return to


def read_config_file(filename):
    config = ConfigParser.ConfigParser()
    config.read(filename)
    return config

def config_section_map(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def get_filename_from_path(path):
    return ntpath.basename(path)




if __name__ == '__main__':
    pass
