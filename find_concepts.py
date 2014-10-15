'''
Created on Oct 13, 2014

@author: gupta
'''

import os.path
import json
import sys
import datetime
import time
import subprocess
from multiprocessing import Pool 
import Queue
import multiprocessing
from collections import defaultdict
from ahocorasick import getTagger
from database import *
from utils import *
import timeout_decorator

con = None
thread_num = 4
gap_3_months = 7948858
endtime = 1413219505  # 09 Oct 2014
kpex_count = 9999
kpex_path = "/home/Amit/kpex_linux/kpex"
kpex_queue = Queue.Queue()
config = read_config_file(get_absolute_path("config.ini"))


seed_concepts = ["climate change"]
seed_tagger = getTagger(seed_concepts)

        
stoplists = []
with open("data/stoplists") as fp:
    for line in fp:
        stoplists.append(line.strip())

               
@timeout_decorator.timeout(1)
def update_concepts_table(concept,score):
#    sorted_update_list = sorted(update_list, key = lambda x: x[0])
#    for concept,score in sorted_update_list:
        print "Concept -->" + str(concept) + " " + str(score)
        print "select * from topsy_temp where name = \"" + concept + "\""
        row_concept = general_select_query(con, 0, "select * from topsy_temp where name = \"" + concept + "\"")    
        if row_concept:
            print "update --> " + str(row_concept) + "  " + str(score)
            update_table(con, "0", "topsy_temp", {"totalcount":int(row_concept['totalcount']) + 1,"overallscore":float(row_concept["overallscore"]) + score}, {"name":concept})
        else:
            print "Inserting Concept --> " + concept
            insert_into_table(con, "0", "topsy_temp",{"name":concept,"totalcount":1,"overallscore": score},insert_ignore = True)
        
                
def run_kpex(filename):
    print "Running kpex on " + filename
    kpex_filename = filename + ".kpex_n" + str(kpex_count) + ".txt"
    if not check_file_exists(kpex_filename):
        cmd = kpex_path + " --to-file --max-len 3 -n " + str(kpex_count) + " " + filename
        try:
            subprocess.check_call(cmd,shell=True)
        except Exception, e:
            return ("exception",e)
    seed_score = compute_seed_score(filename)
    if check_file_exists(kpex_filename):    
        count = 0
        update_list = []
        with open(kpex_filename) as fp:
            for line in fp:
                count += 1
                line = line.strip().lower()
                tokens = line.split()
                if alpha_string(line) and all ([(x not in stoplists) for x in tokens]):
                    update_list.append((line,seed_score + 10000 - count))
        
        sorted_update_list = sorted(update_list, key = lambda x: x[0])
        for concept,score in sorted_update_list:
            for retry in range(0,3): 
                try:
                    update_concepts_table(concept,score)
                    break
                except:
                    pass 


def download_topsy_tweets(keyword,count,endtime):
    s = "http://otter.topsy.com/search.js?callback=jQuery1830654582932125777_1411571964536&q=%22" + keyword.replace(" ","+") +"%22&type=tweet&offset=0&perpage=100&"
    
    download_count = 0
    ans_list = []
    cur_time = endtime
    print "Downloading data for " + keyword
    dictv = {}
    sum_tg = 0
    count_tg = 0
    
    while (download_count < count):
        retry_count = 0
        ts = int(time.time()*1000)
        ts2 = ts + 1300
        starttime = cur_time - gap_3_months 
        query = s + "mintime=" + str(starttime) + "&maxtime=" + str(cur_time) + "&sort_method=-date&call_timestamp=" + str(ts) + "&apikey=09C43A9B270A470B8EB8F2946A9369F3&_=" + str(ts2)
        data_received = False
        r = None
        while (retry_count < 5 and not data_received):
            try:
                r = get_request(query)
                data_received = True
            except timeout_decorator.TimeoutError as e:
                print "Timed Out"
                time.sleep(10)
                retry_count += 1
                continue
            
        
        if data_received and r:
            a = r.text
            c = a.find("{")
            j = a[c:-2]
            js = json.loads(j)
            lent = len(js['response']['list'])
            if lent == 0:
                break
            startt = js['response']['list'][0]['trackback_date']
            endt = js['response']['list'][-1]['trackback_date']
            sum_tg  += (int(endt) - int(startt)) / lent
            count_tg += 1
            for tweet in js['response']['list']:
                text = tweet['content']
                if text[0:50] not in dictv:
                    dictv[text[0:50]] = 1
                    download_count += 1
                    ans_list.append(tweet)
        else:
            break
        
        
            
        cur_time = starttime
    rate = 0    
    if count_tg > 0:
        rate = sum_tg * 1.0 / count_tg    
    return ans_list,rate    



def compute_seed_score(clean_filename):
    total = 0
    seed_score = 0
    with open(clean_filename) as fp:
        for line in fp:
            seed_score += len(seed_tagger.tag(line))
            total += len(line.split())
    
    if total == 0:
        return 0
    
    return int((seed_score*1000.0)/total)        

def get_topsy_tweets(data):
    keyword,tweet_count = data
    filename = "data2/tweets/" + keyword.replace(" ","_") + "_" + str(tweet_count) + ".txt"
    clean_filename = filename + "_clean"
    tweets = []
    if not check_file_exists(filename):
        with open(filename,"w") as fp:
            tweets,rate = download_topsy_tweets(keyword,tweet_count,endtime)
            update_table(con, "0", "topsy_temp", {"frequency":rate}, {"name":keyword}, debug=False)
            for tw in tweets:
                fp.write(json.dumps(tw) + "\n")
                
    if check_file_exists(filename):
        if not check_file_exists(clean_filename):
            create_clean_tweets_file(filename,clean_filename)
        
    if check_file_exists(clean_filename):
        return clean_filename
    else:
        return None             
             


def get_concepts_from_database(worker_id,count=4,debug=False):
    try:
        global con
        con = test_and_get_mysql_con(0, con, config, debug)
        con.autocommit(True)
        status,concept_rows = get_multiple_active_rows(con, worker_id, "topsy_temp", count=count,order_by="overallscore desc", debug=True)
        if status == "success":
            download_pool = Pool(len(concept_rows))
            output1 = download_pool.map(get_topsy_tweets,[(x['name'],x['tweets_count']) for x in concept_rows])
            download_pool.close()
            download_pool.join()
            for x in output1:
                if x:
                    kpex_queue.put(x)
                
                
            kpex_filenames_data = []
            while (not kpex_queue.empty() and len(kpex_filenames_data) < thread_num):
                jobv = kpex_queue.get()
                kpex_filenames_data.append(jobv)
            print kpex_filenames_data
            kpex_pool = Pool(len(kpex_filenames_data))
            kpex_pool.map(run_kpex,kpex_filenames_data)
            kpex_pool.close()
            kpex_pool.join() 
    except:
        print_exec_error(worker_id)        
            
        
   
            
if __name__ == '__main__':
    pcount = 0
    machine_name = "m1_"
    get_concepts_from_database( machine_name + str(pcount),count=4, debug=True)                
