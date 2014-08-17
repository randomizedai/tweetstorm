'''
Created on Aug 6, 2014

@author: Amit

All the database handling
'''
import MySQLdb as mdb
from util import *
from datetime import datetime

'''
function for putting a whole file of data into database

Format of file :-
table_name
fields (comma separated)
values (one line for each entry, comma separated)
'''
def insert_file(filename,con,debug=False):
    data = []
    with open(filename,"r") as fp:
        data = fp.readlines()
    tablename = data[0].strip()
    table_format = data[1].strip().split(",")
    with con:
        cur = con.cursor(mdb.cursors.DictCursor)
        for i in range(2,len(data)):
            line = ["\'" + x + "\'" for x in data[i].strip().split(",")]
            try:
                query = "INSERT INTO " + tablename +  "(" + ",".join(table_format) + ") VALUES (" + ",".join(line) + ")" 
                if debug:
                    print query
                cur.execute(query)
            except Exception, e :
                print line, e

def get_mysql_con(worker_id,config,debug=False):
     if debug:
            print "taskid--" + str(worker_id) +" In get_mysql_con "
    
     con = mdb.connect(config.get('Database','servername') , config.get('Database','username'), config.get('Database','password'), config.get('Database','dbname'))
     con.autocommit(True)
     return con

def test_mysql_con(worker_id,con,debug=False):
    try:
         cur = con.cursor()
         cur.execute("SELECT VERSION()")
         if debug:
             print ("taskid--" + str(worker_id) + " MySQL connection test -- " + str(cur.fetchone()))
         return True
    except Exception, e:
         return False
       
def test_and_get_mysql_con(worker_id,con,config,debug=False):
    if test_mysql_con(worker_id,con,debug):
        return con
    return get_mysql_con(worker_id,config,debug)


def create_table (con,table_name, fields_dict,debug = False): 
    cur = con.cursor()
    query = "CREATE TABLE IF NOT EXISTS " + table_name + " ( "
    list_strings = []
    for k,v in fields_dict.items():
        list_strings.append(k + " " + v)
    
    query += ",".join(list_strings)
    query += " )"
    if debug:
        print query
    cur.execute(query)
    cur.close()    
        
 
def general_select_query(con,worker_id,query,count="one",debug=False):
    if debug:
        print "taskid--" + str(worker_id) + "  " + query
    cur = con.cursor(mdb.cursors.DictCursor)
    cur.execute(query)
    ans = None
    if count == "one":
        ans = cur.fetchone()
    else:
        ans = cur.fetchall()
    cur.close()
    return ans            
        
def select_from_table(con,worker_id,table_name,select_cols,bool_dict,order_by=None, limit=None,count="one",having_dict={},debug=False):
    cur = con.cursor(mdb.cursors.DictCursor)
    having_string = bool_string(" HAVING ",create_assignment_string(having_dict, " AND "))
    where_string = bool_string(" WHERE ",create_assignment_string(bool_dict, " AND "))
    order_by_string = bool_string(" ORDER BY ", order_by)
    limit_string = bool_string(" LIMIT ", limit)
    query = "SELECT " + select_cols + " FROM " + table_name + where_string + having_string + order_by_string + limit_string
    if debug:
        print "taskid--" + str(worker_id) + "  " + query
    cur.execute(query)
    ans = None
    if count == "one":
        ans = cur.fetchone()
    else:
        ans = cur.fetchall()    
    cur.close()
    return ans

def update_table(con,worker_id,table_name,update_dict,bool_dict,debug=False):
    cur = con.cursor(mdb.cursors.DictCursor)
    new_update_dict = {}
    for k,v in update_dict.items():
        if update_dict[k] != None:
            new_update_dict[k] = v
    query = "UPDATE " + table_name + " SET " + create_assignment_string(new_update_dict, ",")  + " WHERE " + create_assignment_string(bool_dict," AND ")  
    if debug:
        print "taskid--" + str(worker_id) + "  " + query
    cur.execute(query)
    cur.close()
    
    
def get_id(con,worker_id,table_name,bool_dict,debug=False):
    ans = select_from_table(con, worker_id, table_name, "id", bool_dict, debug=debug)    
    return ans["id"]


def insert_into_table(con,worker_id,table_name,value_dict,debug=False):
    cur = con.cursor(mdb.cursors.DictCursor)
    kv_pairs = [ (x,y) for (x,y) in value_dict.items() if y != None]
    col_name_string = ",".join([x[0] for x in kv_pairs])
    values_string = ",".join([value_string(x[1]) for x in kv_pairs])
    query = "INSERT INTO " + table_name + "(" + col_name_string + ") VALUES ( " + values_string + ")"
    if debug:
        print "taskid--" + str(worker_id) + " " + query
    cur.execute(query)
    cur.close()

        
def get_active_row(con,worker_id,table_name,bool_dict={},debug=False):
    try:
        bool_dict["active_status"] = 0
        ### get row candidate
        row_candidate = select_from_table(con, worker_id, table_name,"*", bool_dict, "last_access", 1, debug=debug)
        if not row_candidate:
            return ("no-active-candidate",None)
        if debug:
            print "taskid--" + str(worker_id) + " Row Candidate For Table " + table_name + " -- "  + str(row_candidate) + "   task_id --" + str(worker_id)
        
        ### get lock
        update_table(con, worker_id, table_name, {"active_status":worker_id}, {"id":row_candidate["id"]}, debug)

        ##get row status
        row_status = select_from_table(con, worker_id, table_name, "active_status", {"id":row_candidate["id"]},debug=debug)            
        if not row_status:
            return ("lock-row-failed",None)
        if debug:
            print "taskid--" + str(worker_id) + " Row Status For Table " + table_name + " -- "  + str(row_status) + "   task_id --" + str(worker_id)
        
        ## see if you got the lock
        if (row_status['active_status'] == worker_id): 
            if debug:
                print "taskid--" + str(worker_id) +" Received Table " + table_name + " -- for Task_id -- " + str(worker_id)  
            return ("success",row_candidate)
    
        return("lock-failed",None)
    except Exception, e:
       print_exec_error(worker_id)
       return ("exception",e)


def reset_old_table(con,worker_id,table_name,debug=False):
    cur = con.cursor(mdb.cursors.DictCursor)
    cur.execute("UPDATE " + table_name + " SET active_status = 0 where last_access < (NOW() - INTERVAL 15 MINUTE)")
    cur.close()    

def reset_table(con,worker_id,table_name,debug=False):
    cur = con.cursor(mdb.cursors.DictCursor)
    cur.execute("UPDATE " + table_name + " SET active_status = 0")
    cur.close() 
    
    
def release_user(con,worker_id,id,debug=False):
    update_table(con, worker_id, "users", {"active_status" : 0, 'last_access' : str(datetime.now())}, {"id" : id}, debug)   
       
def release_keyword(con,worker_id,id,debug=False):
    update_table(con, worker_id, "keywords", {"active_status" : 0, 'last_access' : str(datetime.now())}, {"id" : id}, debug)   

def release_auth(con,worker_id,id,debug=False):
    update_table(con, worker_id, "twitter_auths", {"active_status" : 0, 'last_access' : str(datetime.now())}, {"id" : id}, debug)   

def release_file(con,worker_id,id,size,debug=False):
    update_table(con, worker_id, "files", {"active_status" : 0, 'last_access' : str(datetime.now()), "size" : size}, {"id" : id}, debug)   

def release_query(con,worker_id,query_type,id,debug=False):
    if query_type == "users":
        release_user(con,worker_id,id,debug)
    elif query_type == "keywords":
        release_keyword(con,worker_id,id,debug)    

    
def get_user(con,worker_id,debug=False):
    ru_status,row_user = get_active_row(con,worker_id,"users",bool_dict={},debug=debug)
    if ru_status == "success" and row_user:
        return row_user
    else:
        print "taskid--" + str(worker_id) + "  In getting User -->" + ru_status
    
       
def get_keyword(con,worker_id,debug=False):
    rk_status,row_kw = get_active_row(con,worker_id,"keywords",bool_dict={},debug=debug)
    if rk_status == "success" and row_kw:
        return row_kw
    else:
        print "taskid--" + str(worker_id) + "  In getting Keyword -->" + rk_status

def get_auth(con,worker_id,debug=False):
    auth_status,row_auth = get_active_row(con,worker_id,"twitter_auths",bool_dict={},debug=debug)
    if auth_status == "success" and row_auth:
        return row_auth   
    else:
        print "taskid--" + str(worker_id) + "  In getting Auth -->" + auth_status


def get_file(con,worker_id,download_dir,machine_name,date,hour,chunk_size,debug=False):
    bool_dict = {"machine_name" : machine_name, "date_string" : date, "hour_string" : hour, "size<":chunk_size}
    file_status,file_row = get_active_row(con,worker_id,"files",bool_dict,debug)
    if file_status != "success":
        value_dict = {}
        filename = "tweets_" + get_random_uuid()  + ".txt"
        value_dict["path"] = download_dir + "/" + date + "/" + hour + "/" + filename
        value_dict["machine_name"] = machine_name
        value_dict["filename"] = filename
        value_dict ["date_string"] = date
        value_dict["size"] = 0
        value_dict["hour_string"] = hour
        value_dict["active_status"] = worker_id
        insert_into_table(con, worker_id, "files", value_dict, debug)
        file_row = select_from_table(con, worker_id, "files", "*", value_dict, debug=debug)
        if file_row and file_row["active_status"] == worker_id:
            file_status = "success"
    return file_status,file_row    
        
if __name__ == '__main__':
    con = None
    
