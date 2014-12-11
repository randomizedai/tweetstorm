#!/usr/bin/env python
import os, sys, json, getopt, codecs
# python xxx.py -y 'tweet' < tweets_jsons.txt > output.txt 
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR + '/utils/')
from article_to_issue import *
from concept_occurrence import *
import time

argv = sys.argv[1:]
try:
    opts, args = getopt.getopt(argv, "hf:y:e:i:a:p:v:n:r", ["file=", "num_pages=", "text=", "type=", "title=", "page_size=", "abstract=", "verbalmappath="])
except getopt.GetoptError:
    print 'article_to_issue.p -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any> -v <verbal path unless in current directory> [-r (for term representation)]'
    sys.exit(2)

file_type = 'tweet'
title = None
text = None
abstract = None
file_path = None
verbal_path = ""
num_pages = []
issue_term_representation = 0
page_size = str(100)
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
        num_pages = [int(el) for el in str(arg).split(',')]
    elif opt in ("-p", "--page_size"):
        page_size = str(arg)
    elif opt in ("-a", "--abstract"):
        abstract = arg
    elif opt in ("-e", "--text"):
        text = arg
    elif opt in ("-v", "--verbalmappath"):
        verbal_path = arg
    elif opt in ("-r"):
        issue_term_representation = 1

verbal_map = read_verbal_ontology(BASE_DIR + "/../../data/")
labels_map, hierarchy, topics = read_topic_to_json_from_db(path='http://146.148.70.53/topics/list/?page_size=1000&concepts=1', dir_maps=BASE_DIR+'/../../data/')
rest_url_extracted_already = 'http://146.148.70.53/concepts/document/'
result = {}
triplets = issues_to_map(issues)
if file_type == 'tweet':
    # text should be in format of json to load tweet
    if text == None:
        for row in sys.stdin:
            indicator, _ = get_indicator_body_title_abstact(file_path, file_type, row, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics)
            if indicator is None:
                continue
            result.update(indicator)
    else:
        indicator, _ = get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics)
        if indicator is not None:
            result.update(indicator)
    print ("\n".join([json.dumps({k:v}) for k, v in result.items()]))
    exit(1)
elif file_type == 'tweet_db':
    file_type = 'tweet'
    articles = articles_to_map("http://146.148.70.53/documents/list/?type=twitter&full_text=1&page_size=100&page="+str(num_pages[0]+1), "http://146.148.70.53/documents/", num_pages )
    for k, v in articles.items():
        text = json.dumps({'text': v['body'], 'id_str': str(k)})
        indicator, _ = get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics)
        if indicator is not None:
            result.update(indicator)
elif file_type == 'news':
    general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/1_climate_keyphrases_aggr_filtered_844') # 1_climate_keyphrases_aggr_filtered_844, amitlist.csv
    articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size="+page_size+"&page="+str(num_pages[0]+1), "http://146.148.70.53/documents/", num_pages )
    devotion = {}
    for k, v in articles.items():
        text = v['body']
        title = v['title']
        doc_id = str(k)
        # if given a json with metadata then use id as file_path
        # terms_per_issues[id_element][key(id of the issues)] = terms_per_issue (tag_list)
        indicator, terms_per_issues = get_indicator_body_title_abstact(doc_id, file_type, text, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics)
        if indicator is None:
            continue
        result.update(indicator)
        if issue_term_representation:
            from synonyms_utils import jaccardGivenScore
            from collections import Counter
            # docs - docs[doc_id]: {k (term_norm): v (count of the term) }
            news = get_occurrences_in_text(indicator=indicator[doc_id], 
                doc_id=doc_id,
                file_type=file_type, 
                text=text, 
                title=title, 
                general_concepts_map=general_concepts_map,
                rest_url_extracted_already=rest_url_extracted_already)
            # docs - docs[doc_id]: { issue_id : [{k (term_norm): v (count of the term) }] }
            for k, v in terms_per_issues[doc_id].items():
                terms_per_issues[doc_id][k] = dict(Counter(v))
            for k, v in terms_per_issues[doc_id].items():
                if doc_id not in devotion:
                    devotion[doc_id] = {}
                devotion[doc_id][k] = jaccardGivenScore(terms_per_issues[doc_id][k], news[doc_id], dict(indicator[doc_id])[k])
            print ">>>>>>>>>>>>>>>>>>>>>>>>>>>"
            print title.encode('utf-8','replace')   
            print "Concentration"        
            print("\n".join([json.dumps({ " ".join([triplets[k][4], triplets[k][2], triplets[k][3]]) :v}) for k, v in devotion[doc_id].items()]))
            print "Identified issue score"
            print("\n".join([json.dumps({ " ".join([triplets[k][4], triplets[k][2], triplets[k][3]]) :v}) for k, v in indicator[doc_id]]))
            print "Terms extracted"
            print sorted( [ (k,v) for k, v in news[doc_id].items()], key=lambda x:x[1], reverse=True)
elif file_type == 'scientific':
    general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/1_climate_keyphrases_aggr_filtered_844') # 1_climate_keyphrases_aggr_filtered_844, amitlist.csv
    articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&full_text=1&page_size=" + page_size+"&page="+str(num_pages[0]+1), "http://146.148.70.53/documents/", num_pages )
    devotion = {}
    for k, v in articles.items():
        text = v['body']
        title = v['title']
        doc_id = str(k)
        # if given a json with metadata then use id as file_path
        indicator, terms_per_issues = get_indicator_body_title_abstact(doc_id, file_type, text, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics)
        if indicator is None:
            continue
        result.update(indicator)
        if issue_term_representation:
            from synonyms_utils import jaccard
            from collections import Counter
            # docs - docs[issue_id] = [(k (term_norm), v (count of the term) )]
            arts = get_occurrences_in_text(indicator=indicator[doc_id], 
                doc_id=doc_id,
                file_type=file_type, 
                text=text, 
                title=title, 
                general_concepts_map=general_concepts_map,
                rest_url_extracted_already=rest_url_extracted_already)
            for k, v in terms_per_issues[doc_id].items():
                terms_per_issues[doc_id][k] = dict(Counter(v))
            for k, v in terms_per_issues[doc_id].items():
                if doc_id not in devotion:
                    devotion[doc_id] = {}
                devotion[doc_id][k] = jaccardGivenScore(terms_per_issues[doc_id][k], arts[doc_id], dict(indicator[doc_id])[k])
timestr = time.strftime("%Y%m%d/%H/")
d = BASE_DIR + '/work/' + str(timestr)
if not os.path.exists(d):
    os.makedirs(d)
if len(result.keys()) > 0 and not issue_term_representation:
    open(d + file_type + "_" + "_".join([str(num_pages[0]), str(num_pages[1])]) + '.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in result.items()]))
if issue_term_representation:
    open(d + file_type + "_" + "devotion" + "_" + "_".join([str(num_pages[0]), str(num_pages[1])]) + '.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in devotion.items()]))
