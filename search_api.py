'''
Created on Aug 6, 2014

@author: Amit

For making calls to Search API of Twitter
'''


from birdy.twitter import *
from util import *
from database import *
import time
import timeout_decorator

def get_client(auth):
    return UserClient(auth['consumer_key'],auth['consumer_secret'],auth['access_token'], auth['access_token_secret'])

@timeout_decorator.timeout(180)
def get_user_tweets(client, worker_id, sinceid=None,maxid=None,debug=False,userid=None,screenname=None):
    if screenname == None and userid == None:
        if debug:
            print "taskid--" + str(worker_id) + " Bad Query -- both userid and screenname missing"
            return ("exception","Bad Query")
    elif screenname:            
        try:
            if debug:
                print "taskid--" + str(worker_id) + "  User Query -->  screenname -- " + screenname + " sinceid -- " + str(sinceid) + " maxid -- " + str(maxid)  
            response = client.api.statuses.user_timeline.get(screen_name=screenname,since_id=sinceid,max_id=maxid,count=200)
            if debug:
                print "taskid--" + str(worker_id) + " User Query -- Header --> " + str(response.headers)
        
            if response.data:
                return ("success",response.data)
            else:
                return ("no-data",response)
        except Exception, e:
            return ("exception",e)
    else:
        try:
            if debug:
                print "taskid--" + str(worker_id) + "  User Query -->  userid -- " + userid + " sinceid -- " + str(sinceid) + " maxid -- " + str(maxid)  
            response = client.api.statuses.user_timeline.get(user_id=userid,since_id=sinceid,max_id=maxid,count=200)
            if debug:
                print "taskid--" + str(worker_id) + " User Query -- Header --> " + str(response.headers)
        
            if response.data:
                return ("success",response.data)
            else:
                return ("no-data",response)
        except Exception, e:
            return ("exception",e)

@timeout_decorator.timeout(180)
def get_keyword_tweets(client,search_keyword,worker_id,sinceid=None,maxid=None,debug=False):
    try:
        if debug:
            print "taskid--" + str(worker_id) + "  Keyword Query -->  keyword -- " + search_keyword + " sinceid -- " + str(sinceid) + " maxid -- " + str(maxid)  
        response = client.api.search.tweets.get(q=search_keyword,since_id=sinceid,max_id=maxid,count=200)
        if debug:
           print "taskid--" + str(worker_id) + " Keyword Query -- Header --> " + str(response.headers) 
        if response.data.values():
            ans_tweets = response.data.values()[1]
            return ("success",ans_tweets)
        else:
            return ("no-data",response)
    except Exception, e:
        return ("exception",e)

def pick_search_query(client,query_type,query,worker_id,sinceid=None,maxid=None,debug=False):
    try:
        if query_type == "users":
            return get_user_tweets(client,worker_id, sinceid, maxid,debug,screenname=query['screenname'],userid=query['userid'])
        elif query_type == "keywords":
            return get_keyword_tweets(client,query['keyword'], worker_id, sinceid, maxid,debug)
    except Exception,e:
        return "no-data",[]


def get_search_tweets_recursive(client,worker_id,query_type,query,wait_time_in_seconds=5.5,num_tries=50,sinceid=None,maxid=None,debug=False):
    if debug:
        if sinceid and maxid:
            print "taskid--" + str(worker_id) + " Since-Max Query == " + sinceid + "--" + maxid
            if maxid < sinceid:
                print "Weird query -- maxid < sinceid "
                return "exception", "Weird query -- maxid < sinceid"
        elif sinceid:
            print "taskid--" + str(worker_id) + " Since Query == " + sinceid
        elif maxid:
            print "taskid--" + str(worker_id) + " Max Query == " + maxid
        else:
            print "taskid--" + str(worker_id) + " init Query --> No max and since id"
    ans_tweets = []
    status = "exception"
    for _ in range(num_tries):    
        status, response = pick_search_query(client,query_type,query,worker_id,sinceid=sinceid,maxid=maxid,debug=debug)
        
        if debug:
            print "taskid--" + str(worker_id) + " Waiting for " + str(wait_time_in_seconds) + " seconds for further tweets"
            print "taskid--" + str(worker_id) + " sinceid--" + str(sinceid) + "  maxid--" + str(maxid)
            print "taskid--" + str(worker_id) + " Total tweets--" + str(len(ans_tweets))
    
        time.sleep(wait_time_in_seconds)    
        
        if status == "success":
            ans_tweets += response
            new_min_id = compute_min_id(response)
            new_max_id = compute_max_id(response)
            
            if debug and response:
                print "taskid--" + str(worker_id) + " New tweets--" + str(len(response))
                print "taskid--" + str(worker_id)  + " New Max Id--" + str(new_max_id) + " New Min Id--" + str(new_min_id)
            if maxid and sinceid: ## both query (see the response to decide)
                if sinceid == new_min_id and maxid == new_max_id:
                    break
                elif sinceid == new_min_id:
                    sinceid = new_max_id    
                elif maxid == new_max_id:
                    maxid = new_min_id
                else:
                    sinceid = new_max_id
                             
            elif sinceid:
                sinceid = new_max_id ## since id query
            elif maxid:
                maxid = new_min_id ## maxid query 
            else:
                maxid = new_min_id  ## beginning query, make it a since one        
        
        if status != "success" or len(response) == 1:
            break
             
    return status,ans_tweets    
       

if __name__ == '__main__':
    con = None
    config = read_config_file(get_absolute_path("config.ini"))
    debug = True
    worker_id = 1
    con = test_and_get_mysql_con(worker_id, con, config, debug)
    auth = get_auth(con, worker_id, debug)
    client = get_client(auth)
    get_search_tweets_recursive(client, 1, "users",{"userid":11695472},debug=debug)
    release_auth(con, worker_id, auth['id'], debug)  