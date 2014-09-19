'''
Created on Sep 17, 2014

@author: Amit
'''
import os
from util import *
import subprocess

def compute_feature(feature_row,file_row,worker_id,debug=False):
    input_file_path = file_row['path']
    filename = get_filename_from_path(input_file_path)
    filename_suffix = filename[len(feature_row['input_feature']):]
    output_filename = feature_row['output_feature'] + filename_suffix
    output_file_path = os.path.dirname(input_file_path) + "/" + output_filename
    tempval = get_random_uuid()
    temp_output_file_path = os.path.dirname(input_file_path) + "/_tmp_" + tempval
    feature_command = feature_row['command'] + " < " + input_file_path + " > " + temp_output_file_path
    if debug:
        print "taskid--" + str(worker_id) +  "Feature command --> " + feature_command
        print "taskid--" + "mv " + temp_output_file_path + " " + output_file_path
    
    try:
        p1  = subprocess.check_call(feature_command,shell=True)
        p2 = subprocess.check_call("mv " + temp_output_file_path + " " + output_file_path,shell=True )
        return ("success",(output_file_path,output_filename))
    except Exception, e:
        return ("exception",e)
        
            
if __name__ == '__main__':
    pass