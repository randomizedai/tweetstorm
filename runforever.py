'''
Created on Aug 10, 2014

@author: Amit
'''
import sys
import time
from main import *
import MySQLdb as mdb
from util import *
from database import *
from monitoring import *
from celery.task.control import discard_all

config = read_config_file(get_absolute_path("config.ini"))




def main_loop(debug=False):
    con = None
    con = test_and_get_mysql_con(0, con, config, debug)
    ans1 = select_from_table(con,0,"download_logs","MAX(worker_id) as max",{},debug=True)
    ans2 = select_from_table(con,0,"twitter_auths","MAX(active_status) as max",{},debug=True)
    feature_ans1 = select_from_table(con,0,"feature_logs","MAX(worker_id) as max",{},debug=True)
    feature_ans2 = select_from_table(con,0,"features_machines","MAX(active_status) as max",{},debug=True)
    
    count = max(ans1['max'],ans2['max']) + 1 
    feature_count = max(feature_ans1['max'],feature_ans2['max']) + 1
    generate_report_nth_hour = 24
    clean_auths_hour = 0.083  #(5 minutes)
    last_cleaned_auths_time = time.time()
    last_report_generated_time = time.time()
    worker_id = 0
    first_time = True
    first_report = True
    while 1:
        cur_time = time.time()
        if (first_report and ( cur_time - last_report_generated_time ) / (60 * 60) > 0.15) or  ( cur_time - last_report_generated_time ) / (60 * 60) > generate_report_nth_hour:
            first_report = False
            try:
                generate_and_send_report(config,last_report_generated_time,cur_time)
                print "generating report at" 
                print cur_time
                last_report_generated_time = cur_time
            except Exception,e:
                print "Exception in Generating report"
                print_exec_error(0)
        
        cur_time = time.time()
        if (cur_time - last_cleaned_auths_time) / (60*60) > clean_auths_hour:
            clean_stuck_auths(con,worker_id,debug=True)
            last_cleaned_auths_time = cur_time      
        
        con = test_and_get_mysql_con(0, con, config,debug=False)
        ans = select_from_table(con,worker_id, "twitter_auths", "COUNT(*) as count", {"active_status" : 0}, count="one", debug=False)
        print get_current_timestamp() + " --> Starting " + str(ans['count']) + " workers"
        lent = ans['count']
        if first_time:
            first_time = False
            lent = 20
        for i in range(0,lent):
            worker_main.delay(count,debug=True)
            compute_feature_main.delay(feature_count, debug=True)
            feature_count = feature_count + 1
            count = count + 1
            time.sleep(0.5)
        print "started"    
        sys.stdout.flush()
        time.sleep(60)

if __name__ == '__main__':
    try:
        main_loop(debug=True)
    except KeyboardInterrupt:
        discard_all()
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)
