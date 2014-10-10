#!/usr/bin/env python
import os, sys, json, getopt, codecs
# python xxx.py -y 'tweet' < tweets_jsons.txt > output.txt 
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR + '/utils/')
from article_to_issue import *

argv = sys.argv[1:]
try:
    opts, args = getopt.getopt(argv, "hf:y:e:i:a:v:", ["file=", "text=", "type=", "title=", "abstract=", "verbalmappath="])
except getopt.GetoptError:
    print 'article_to_issue.py -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any>'
    sys.exit(2)

file_type = 'news'
title = None
text = None
abstract = None
file_path = None
verbal_path = ""
for opt, arg in opts:
    if opt == '-h':
        print 'article_to_issue.p -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any> -v <verbal path unless in current directory>'
        sys.exit()
    elif opt in ("-y", "--type"):
        file_type = arg
    elif opt in ("-f", "--file"):
        file_path = arg
    elif opt in ("-i", "--title"):
        title = arg
    elif opt in ("-a", "--abstract"):
        abstract = arg
    elif opt in ("-e", "--text"):
        text = arg
    elif opt in ("v", "--verbalmappath"):
        verbal_path = arg

verbal_map = read_verbal_ontology(BASE_DIR + "/../../data/")
result = {}
if file_type == 'tweet':
	# text should be in format of json to load tweet
	if text == None:
		for row in sys.stdin:
			result.update(get_indicator_body_title_abstact(file_path, file_type, row, title, abstract, verbal_map))
	else:
		result.update(get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map))
elif file_type == 'news' or file_type == 'paper':
	for row in sys.stdin:
		text = row
		# if given a json with metadata then use id as file_path
		result.update(get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map))

print ("\n".join([json.dumps({k:v}) for k, v in result.items()]))
