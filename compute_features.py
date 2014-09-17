'''
Created on Sep 17, 2014

@author: Amit
'''
import os
from util import *
import subprocess

def compute_feature(feature_row,file_row,worker_id,dir_path,debug=False):
    input_file_path = file_row['path']
    filename = get_filename_from_path(input_file_path)
    filename_suffix = filename[len(feature_row['input_feature']):]
    output_filename = feature_row['output_feature'] + filename_suffix
    output_file_path = os.path.dirname(input_file_path) + "/" + output_filename
    feature_command = feature_row + " < " + input_file_path + " > " + output_file_path
    if debug:
        print "taskid--" + str(worker_id) +  "Feature command --> " + feature_command
    
    try:
        p  = subprocess.Popen(feature_command,shell=True).wait()
        return ("success","")
    except Except, e:
        return ("exception",e)
        
            
if __name__ == '__main__':
    pass