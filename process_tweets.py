'''
Created on Aug 9, 2014

@author: Amit
'''
from utils import *
from database import *
from filesystem import *
from datetime import datetime

def update_max_since_ids(query_type,query,new_minid,new_maxid,con,worker_id,debug=False):
    ## insert queries in case of since/max id
    current_timestamp = str(datetime.now()) 
    ln_min = long(new_minid)
    ln_max = long(new_maxid)        
    if query['since_id'] != None and query['max_id'] != None:
        lqs = long(query['since_id'])
        lqm = long(query['max_id'])
        
        if lqs < ln_min:
            new_query = dict(query)
            new_query.pop("id",None)
            new_query['max_id'] = str(new_minid) 
            new_query['last_access'] = current_timestamp
            insert_into_table(con, worker_id, query_type,new_query, debug)
        
        if ln_max < lqm:     
            new_query = dict(query)
            new_query.pop("id",None)
            new_query['since_id'] = str(new_maxid)
            new_query['last_access'] = current_timestamp 
            insert_into_table(con, worker_id, query_type,new_query, debug)

    ## update in case of just since or max id
    elif query['since_id']:
        lqs = long(query['since_id'])
    
        if ln_min > lqs:
            new_query = dict(query)
            new_query.pop("id",None)
            new_query['since_id'] = str(query['since_id'])
            new_query['max_id'] = str(new_minid)
            new_query['last_access'] = current_timestamp 
            insert_into_table(con, worker_id, query_type,new_query, debug)

        dictv = {'since_id' : new_maxid, 'last_access' : current_timestamp}
        update_table(con, worker_id, query_type, dictv, {"id": query["id"]}, debug)
    elif query['max_id']:
        lqm = long(query['max_id'])
        dictv = {'max_id' : new_minid, 'last_access' : current_timestamp }
        update_table(con, worker_id, query_type, dictv, {"id": query["id"]}, debug)
    else:
        dictv = {'since_id' : new_maxid, 'last_access' : current_timestamp  }
        update_table(con, worker_id, query_type, dictv, {"id": query["id"]}, debug)       
        new_query = dict(query)
        new_query.pop("id",None)
        new_query.pop("since_id",None)
        new_query['max_id'] = str(new_minid) 
        new_query['last_access'] = str(datetime.now()) 
        insert_into_table(con, worker_id, query_type,new_query, debug)

def put_download_logs(query_type,query,status,results,worker_id,con,file_id,debug=False):    
    dictv = {}
    dictv['query_type'] = query_type
    dictv['query_id'] = query['id']
    dictv['count'] = 0
    if status =="success":
        dictv['count'] = len(results)
    dictv['download_time'] = str(datetime.now())
    dictv['file_id'] = file_id
    dictv['max_id'] = query['max_id']
    dictv['since_id'] = query['since_id']
    dictv['status'] = status
    dictv['worker_id'] = worker_id
    if status == "exception":
        dictv['exception_description'] = ""
    insert_into_table(con, worker_id, "download_logs", dictv, debug)    

        
def process_output(query_type,query,status, results, worker_id, con, file_handle,file_id,debug=False):
    if query_type == "manual_tweets":
        dump_tweets_into_file(file_handle, results)
        remove_manual_tweets(con,worker_id,results)
        return
    if isinstance(results,list) and len(results) > 0:
        new_minid = compute_min_id(results)
        new_maxid = compute_max_id(results)
        dump_tweets_into_file(file_handle,results)
        update_max_since_ids(query_type,query,new_minid,new_maxid,con,worker_id,debug)
    
    if isinstance(results,list) and len(results) < 2:
        update_table(con,worker_id,query_type,{"retries": query["retries"] + 1},{"id":query["id"]},debug)
   # elif status == "no-data":
   #     update_table(con, worker_id, query_type, {'last_access' : str(datetime.now())   }, {"id": query["id"]}, debug)        
    
    put_download_logs(query_type,query,status,results,worker_id,con,file_id,debug)
        




if __name__ == '__main__':
    pass
