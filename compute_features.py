'''
Created on Sep 17, 2014

@author: Amit
'''
import os
from utils import *
import subprocess

def compute_feature(new_dir,feature_row,file_row,worker_id,debug=False):
    input_file_path = file_row['path']
    tokens = [x for x in input_file_path.split("/") if x]
    new_dir_tokens = [x for x in new_dir if x]
    tokens = new_dir_tokens + tokens[len(new_dir_tokens):]
    input_file_path = "/".join(tokens)
    filename = get_filename_from_path(input_file_path)
    filename_suffix = filename[len(feature_row['input_feature']):]
    output_filename = feature_row['output_feature'] + filename_suffix
    output_file_path = ""
    temp_output_file_path = ""
    feature_command = feature_row['command'].replace("$input",input_file_path)
    dirname = os.path.dirname(input_file_path)
    if feature_row['output_feature']:
        output_file_path = dirname + "/" + output_filename
        tempval = get_random_uuid()
        temp_output_file_path = dirname + "/_tmp_" + tempval
        feature_command = feature_command + " > " + temp_output_file_path
        mkdir_command = "mkdir -p " + dirname
    if debug:
        print "taksid-- " + str(worker_id) + " mkdir -p "
        print "taskid-- " + str(worker_id) +  "Feature command --> " + feature_command
        print "taskid-- " + "mv " + temp_output_file_path + " " + output_file_path
    
    try:
        p1 = subprocess.check_call(mkdir_command,shell=True)
        p1  = subprocess.check_call(feature_command,shell=True)
        if feature_row['output_feature']:
            p2 = subprocess.check_call("mv " + temp_output_file_path + " " + output_file_path,shell=True )
        return ("success",(output_file_path,output_filename))
    except Exception, e:
        return ("exception",e)
        
            
if __name__ == '__main__':
    pass
