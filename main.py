'''
Created on Aug 6, 2014

@author: Amit

Main Celery worker code
'''

import sys, os
import ConfigParser
from database import *
from scheduler import *
from search_api import *
from process_tweets import *
from filesystem import *
from util import *
from celery import Celery
import traceback
from compute_features import *

config = read_config_file(get_absolute_path("config.ini"))
queue_name = config.get("celery","queue_name")
backend_type = config.get("celery","backend_type")
broker_name = config.get("celery","broker")
celery = Celery(queue_name,backend=backend_type,  broker=broker_name)
download_dir = config.get("downloads","download_dir")

con = None
hostname = get_host_name()
wait_time_in_seconds = 5.5
num_retries = 10
chunk_size = 500 #MBs
current_file = None
current_file_id = None

# def manage_file_handle(download_dir,con,worker_id,current_file,current_file_id,debug=False):
#     if debug:
#         if not current_file:
#             print "taskid--" + str(worker_id) + " No current file"
#         else:    
#             print "taskid--" + str(worker_id) + " current filesize --> " + str(filesize(current_file))
# 
#     if  (not current_file) or (current_file and filesize(current_file) > 100):
#         if current_file:
#             current_file.close()
#         if debug:
#             print "taskid--" + str(worker_id) + " making new file for writing"     
#         current_file = create_new_file(download_dir + "/" + get_current_date(), "tweets_")    
#         insert_into_table(con, worker_id, "files", {"machine_name" : hostname, "path" : current_file.name, "filename":get_filename_from_path(current_file.name)}, debug)    
#         current_file_id = get_id(con, worker_id, "files", {"path" : current_file.name, "machine_name" : hostname }, debug)
# 
#     return current_file, current_file_id



# 
# def manage_file_handle(download_dir,con,worker_id,debug=False):
#     if debug:
#         print "taskid--" + str(worker_id) + " Trying to get a file lock"
#     date_download_dir = download_dir + "/" + get_current_date() + "/" + get_current_hour()
#     make_dir(date_download_dir)     
#     list_files = sorted([ x for x in os.listdir(date_download_dir) if x.endswith(".txt") ])
#     if len(list_files) == 0 or filesize(date_download_dir + "/" + list_files[-1]) > chunk_size:
#         new_file = create_new_file(date_download_dir, "tweets_" + get_current_time + ".txt") 
#         lock = get_lock (new_file.name)
        
        
@celery.task        
def worker_main(worker_id,debug=False):
    query_type = None
    query = None
    auth = None
    con = None
    
    try:
        con = test_and_get_mysql_con(worker_id, con, config, debug)
        auth = get_auth(con, worker_id, debug)
        if debug:
            if not auth:
                print "taskid--" + str(worker_id) + "  No Auth Received"
                
            else:
                print "taskid--" + str(worker_id) + " Auth received -- " + str(auth)
        if not auth:
            raise Exception("No Auth received")
        scheduler_func = last_accessed_first
        
        if debug:
            print "taskid--" + str(worker_id) +" Looking for a query "
        # query_type corresponds to table name
        query_type, query = scheduler_func(con,worker_id,debug) 
        if debug:
            if query:
                print "taskid--" + str(worker_id) + " Query --> " +  query_type  + " " +  str(query)
            else:    
                print "taskid--" + str(worker_id) + "  No User/Keyword or Query Received"
        
        client = get_client(auth)    
        status,response = get_search_tweets_recursive(client, worker_id, query_type, query, wait_time_in_seconds, num_retries,query['since_id'],query['max_id'],debug )
        
        if debug:
            print "taskid--" + str(worker_id) + " Status --> " + status + " Downloaded Tweets --> " + str(len(response))
        
        
        file_status,file_row = get_file(con, worker_id, download_dir, hostname, get_current_date(), get_current_hour(), chunk_size, debug)
        if debug:
            print "taskid--" + str(worker_id) + " File Status --> " + file_status
            print "taskid--" + str(worker_id) + " File Name --> " + file_row['path']
            
        if file_status == "success":
            current_file = get_file_handle(file_row['path'])
            current_file_id = file_row['id']
            process_output(query_type,query,status,response,worker_id,con,current_file,current_file_id,debug)
            current_file.close()
            release_file(con, worker_id, file_row['id'], filesize(file_row['path']), debug)
            
        
        release_query(con,worker_id,query_type,query['id'],debug)
        release_auth(con, worker_id, auth['id'], debug)        
    except Exception, e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("********#####EXCEPTION#####********* taskid--"+ str(worker_id),exc_type, fname, exc_tb.tb_lineno)
        traceback.print_exc()
        print "taskid--" + str(worker_id) + " In Exception --",e
        try:
            if file_row:
                release_file(con, worker_id, file_row['id'], filesize(file_row['path']), debug)        
        except Exception, e:
            pass
        try:
            if query_type and query:
                release_query(con,worker_id,query_type,query['id'],debug)
        except Exception, e:
            pass
        try:
            if auth:
                release_auth(con, worker_id, auth['id'], debug)        
        except Exception, e:
            pass
                
 
@celery.task        
def compute_feature_main(worker_id,debug=False):
    con = None
    
    try:
        con = test_and_get_mysql_con(worker_id, con, config, debug)
        insert_features_machines(con, worker_id, hostname, debug)
        machine_id = get_machine_id(hostname, con, worker_id, debug)
        fm = get_feature_machine_pair(con, worker_id, machine_id, debug)
        feature =  select_from_table(con, worker_id, "features", "*", {"id":fm['feature_id']},count="one",debug=debug)
        
        if debug:
            print "taskid--" + str(worker_id) + "fm pair -->" + str(fm)
        list_files = get_files_for_fm_pair(con, worker_id, fm, debug=debug)
        if debug:
            print "taskid--" + str(worker_id) + "list of files -->" + str(list_files)
   
        list_log_ids = insert_files_into_feature_logs(con, worker_id, list_files, fm['id'], debug)    
        if debug:
            print "taskid--" + str(worker_id) + "list of log_ids -->" + str(list_log_ids)
   
        for i,file in enumerate(list_files):
            try:
                status,message = compute_feature(feature, file, worker_id, debug)
                if status == "success":
                    insert_into_table(con, worker_id, "files", {"machine_name":hostname,"path":message[0],"filename":message[1]\
                    ,"date_string":get_current_date(),"hour_string":get_current_hour(),"last_access":str(get_current_timestamp())}, debug)
                    update_feature_logs(con, worker_id, list_log_ids[i], status, "", debug)
                else:
                    update_feature_logs(con, worker_id, list_log_ids[i], status, con.escape_string(str(message)), debug)    
            except Exception,e:
                update_feature_logs(con, worker_id, list_log_ids[i], "exception", con.escape_string(str(e)), debug)
               
        
        release_feature_machine_pair(con, worker_id, fm['id'], debug)

    except Exception, e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("********#####EXCEPTION#####********* taskid--"+ str(worker_id),exc_type, fname, exc_tb.tb_lineno)
        traceback.print_exc()
        print "taskid--" + str(worker_id) + " In Exception --",e
        try:
            if fm:
                release_feature_machine_pair(con, worker_id, fm['id'], debug)
        except Exception, e:
            pass
 
 
if __name__ == '__main__':
    worker_main(1,debug=True)
    