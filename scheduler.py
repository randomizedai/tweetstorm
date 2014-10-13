'''
Created on Aug 6, 2014

@author: Amit

Scheduling Algorithms to pick the next query
'''
from database import *
from utils import *
from random import randint
# query type corresponds to table_name
def last_accessed_first(con,worker_id,debug=False):
    try:
        user = get_user(con, worker_id, debug)
        if debug:
            print "taskid--" + str(worker_id) +" got user " + str(user)
        
        keyword = get_keyword(con, worker_id, debug)
        if debug:
            print "taskid--" + str(worker_id) +" got keyword " + str(keyword)
    
        
        if user and keyword:
            x = randint(0,100)
      #      if x
      #      user_last_access_time = user['last_access']
      #      keyword_last_access_time = keyword['last_access']
   #         if (user_last_access_time == None and keyword_last_access_time) or (user_last_access_time < keyword_last_access_time):
            if x < 60:     
                release_keyword(con, worker_id, keyword["id"], debug,last_access=False)
                return ("users",user)
            else:
                release_user(con, worker_id, user["id"], debug,last_access=False)
                return ("keywords",keyword)
        elif keyword:
            return ("keywords",keyword)
        elif user:
            return ("users",user)
        return
    except Exception, e:
        print_exec_error(worker_id)
        try:
            if user:
                release_user(con, worker_id, user["id"],debug)
        except Exception, e:
             pass                
    
        try:
            if keyword:
                release_keyword(con, worker_id, keyword["id"], debug)
        except Exception, e:
             pass                
    
    
if __name__ == '__main__':
    pass
