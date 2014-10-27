#!/usr/bin/env python
import os, sys, json, getopt, codecs
# python xxx.py -y 'tweet' < tweets_jsons.txt > output.txt 
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR + '/utils/')
from article_to_issue import *
from concept_occurrence import *

argv = sys.argv[1:]
try:
    opts, args = getopt.getopt(argv, "hf:y:e:i:a:v:n:r", ["file=", "num_pages=", "text=", "type=", "title=", "abstract=", "verbalmappath="])
except getopt.GetoptError:
    print 'article_to_issue.py -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any>'
    sys.exit(2)

file_type = 'tweet'
title = None
text = None
abstract = None
file_path = None
verbal_path = ""
num_pages = []
issue_term_representation = 0
for opt, arg in opts:
    if opt == '-h':
        print 'article_to_issue.p -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any> -v <verbal path unless in current directory> [-r (for term representation)]'
        sys.exit()
    elif opt in ("-y", "--type"):
        file_type = arg
    elif opt in ("-f", "--file"):
        file_path = arg
    elif opt in ("-i", "--title"):
        title = arg
    elif opt in ("-n", "--num_pages"):
        num_pages = [0, int(arg)]
    elif opt in ("-a", "--abstract"):
        abstract = arg
    elif opt in ("-e", "--text"):
        text = arg
    elif opt in ("-v", "--verbalmappath"):
        verbal_path = arg
    elif opt in ("-r"):
        issue_term_representation = 1

verbal_map = read_verbal_ontology(BASE_DIR + "/../../data/")
result = {}
if file_type == 'tweet':
	# text should be in format of json to load tweet
	if text == None:
		for row in sys.stdin:
            indicator = get_indicator_body_title_abstact(file_path, file_type, row, title, abstract, verbal_map)
            if indicator is None:
                continue
			result.update(indicator)
	else:
        indicator = get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map)
        if indicator is None:
            continue
		result.update()
elif file_type == 'news':
    general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/1_climate_keyphrases_aggr_filtered_844') # 1_climate_keyphrases_aggr_filtered_844, amitlist.csv
    articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&page_size=100", "http://146.148.70.53/documents/", num_pages )
    news = {}
    terms_index = {}
    counter = 0
    for k, v in articles.items():
        text = v['body']
        title = v['title']
        doc_id = str(k)
        # if given a json with metadata then use id as file_path
        indicator = get_indicator_body_title_abstact(doc_id, file_type, text, title, abstract, verbal_map)
        if indicator is None:
            continue
        result.update(indicator)
        if issue_term_representation:
            for issue_scores in indicator[doc_id]:
                if issue_scores[1] > 0:
                    occurrence = ConceptOccurrence(text.lower(), 'news')
                    occurrence.title = title
                    occurrence.get_occurrence_count({}, {}, general_concepts_map)
                    terms = {}
                    if len(occurrence.preprocessed) > 1:
                        news[doc_id] = {'issue': issue_scores[0]}
                        for el in occurrence.preprocessed:
                            if el in terms:
                                terms[el] += 1
                            else:
                                terms[el] = 1
                            if el not in terms_index:
                                terms_index[el] = counter
                                counter += 1
                        news[doc_id]['concepts'] = sorted([(terms_index[k], v) for k,v in terms.items()], key=lambda x:x[1], reverse=True)
    if issue_term_representation:
        print ("\n".join([json.dumps({k:v}) for k, v in news.items()]))
        print "-------------------"
        print ("\n".join([json.dumps({k:v}) for k, v in terms_index.items()]))
elif file_type == 'scientific':
    general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/1_climate_keyphrases_aggr_filtered_844') # 1_climate_keyphrases_aggr_filtered_844, amitlist.csv
    articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&page_size=100", "http://146.148.70.53/documents/", num_pages )
    arts = {}
    terms_index = {}
    counter = 0
    for k, v in articles.items():
        text = v['body']
        title = v['title']
        doc_id = str(k)
        # if given a json with metadata then use id as file_path
        indicator = get_indicator_body_title_abstact(doc_id, file_type, text, title, abstract, verbal_map)
        if indicator is None:
            continue
        result.update(indicator)
        if issue_term_representation:
            for issue_scores in indicator[doc_id]:
                if issue_scores[1] > 0:
                    occurrence = ConceptOccurrence(text.lower(), 'scientific')
                    occurrence.title = title
                    occurrence.get_occurrence_count({}, {}, general_concepts_map)
                    terms = {}
                    if len(occurrence.preprocessed) > 1:
                        arts[doc_id] = {'issue': issue_scores[0]}
                        for el in occurrence.preprocessed:
                            if el in terms:
                                terms[el] += 1
                            else:
                                terms[el] = 1
                            if el not in terms_index:
                                terms_index[el] = counter
                                counter += 1
                        arts[doc_id]['concepts'] = sorted([(terms_index[k], v) for k,v in terms.items()], key=lambda x:x[1], reverse=True)
    if issue_term_representation:
        print ("\n".join([json.dumps({k:v}) for k, v in arts.items()]))
        print "-------------------"
        print ("\n".join([json.dumps({k:v}) for k, v in terms_index.items()]))
print ">>>>>>>>>>>>>>>>>>>>>>>>>>>"
print ("\n".join([json.dumps({k:v}) for k, v in result.items()]))
