#!/usr/bin/env python
import os, sys, json, getopt, codecs
# python xxx.py -y 'twitter' < tweets_jsons.txt > output.txt 
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR + '/utils/')
from concept_occurrence import *
import time

argv = sys.argv[1:]
try:
	opts, args = getopt.getopt(argv, "hy:n:", ["type=", "num_pages="])
except getopt.GetoptError:
	print 'topic_vectors_relevance.py -y <type of the file> -n <number of json pages to process for news/sci articles>'
	sys.exit(2)

file_type = 'twitter'
num_pages = []
for opt, arg in opts:
	if opt == '-h':
		print 'topic_vectors_relevance.py -y <type of the file [twitter, news, paper]> -n <number of json pages to process for news/sci articles>'
		sys.exit()
	elif opt in ("-y", "--type"):
		file_type = arg
	elif opt in ("-n", "--num_pages"):
		num_pages = [int(el) for el in str(arg).split(',')]

docs_occurrence = {}
# labels_map and hierarchy is used for assigning preliminary topics to the docs
# TODO: be able to read hierarchy in any order
hierarchy = {} #json.loads(open(BASE_DIR + '/../../data/hierarchy_for_topics.json', 'r').read())
# labels_map, hierarchy, topics = read_topic_to_json_from_dir(BASE_DIR + '/../../data/topics/')
labels_map, hierarchy, topics = read_topic_to_json_from_db('http://146.148.70.53/topics/list/')
# labels_map = json.loads(open(BASE_DIR + '/../../data/top_concepts.json', 'r').read()) # concepts_with_synonyms.concepts_for_topics.json
# Labels that are used to construct the docs -> terms vectors
# general_concepts_map is additionally enriched with occurrence of the concepts from the labels_map
general_concepts_map = {} # load_csv_terms(BASE_DIR + '/../../data/amitlist.csv') # 1_climate_keyphrases_aggr_filtered_844

if file_type == "twitter":
	# tweets = tweets_to_map("http://146.148.70.53/tweets/list/", "http://146.148.70.53/tweets/", num_pages)
	# directory = BASE_DIR + "/../../data/julia_llda/"
	# tweets = read_from_multiple_files(directory)
        sys.setrecursionlimit(10000)
	for row in sys.stdin:
		v = json.loads(row)
		k = v['id_str']
		# for k, v in tweets.items(): #open(BASE_DIR + "/../demo.json", 'r').readlines():
		occurrence = ConceptOccurrence(v['text'], file_type)
		occurrence.get_occurrence_count(labels_map, hierarchy, general_concepts_map)
		docs_occurrence[str(k)] = occurrence.struct_to_map(hierarchy, topics)

	for k, v in docs_occurrence.items():
		if 'labels' in v:
			scores = []
			for pairs in v['labels']:
				scores.append([ labels_map[pairs[0]][2], pairs[1] ])
			if scores:
				print json.dumps({k : scores})

	# model_path = wrap_llda(docs_occurrence)
	# topic_vector_map = read_topic_vectors(model_path, general_concepts_map, labels_map, file_type)
	# # tweets
	# document_topic_relevance = rank_element_to_topics(model_path, labels_map, docs_occurrence, tweets)

	# open(BASE_DIR + '/work/docs_occurrence1.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in docs_occurrence.items()]))
	# open(BASE_DIR + '/work/topic_vector_map1.json', 'w').write("\n\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	# open(BASE_DIR + '/work/document_topic_relevance1.json', 'w').write("\n\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))
	# print("\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	# print("\n\n")
	# print("\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))

elif file_type == "news":
	articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size=10", "http://146.148.70.53/documents/", num_pages)
	for k, v in articles.items():
		occurrence = ConceptOccurrence(v['body'], file_type)
		occurrence.title = v['title']
		occurrence.get_occurrence_count(labels_map, hierarchy, general_concepts_map)
		docs_occurrence[str(k)] = occurrence.struct_to_map(hierarchy, topics)

	# model_path = wrap_llda(docs_occurrence)
	# topic_vector_map = read_topic_vectors(model_path, general_concepts_map, labels_map)
	# try:
	# 	document_topic_relevance = rank_element_to_topics(model_path, labels_map, docs_occurrence, {})
	# except Exception, e:
	# 	print e

	# print("\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	# print("\n\n")
	# print("\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))

elif file_type == "enb":
	pass
elif file_type == "scientific":
	articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&full_text=1&page_size=100", "http://146.148.70.53/documents/", num_pages)
	for k, v in articles.items():
		occurrence = ConceptOccurrence(v['body'], file_type)
		occurrence.title = v['title']
		occurrence.get_occurrence_count(labels_map, hierarchy, general_concepts_map)
		docs_occurrence[str(k)] = occurrence.struct_to_map(hierarchy, topics)

if file_type != 'twitter':
	timestr = time.strftime("%Y%m%d/%H/")
	d = BASE_DIR + '/work/topics/' + str(timestr)
	if not os.path.exists(d):
		os.makedirs(d)

	sc = []
	for k, v in docs_occurrence.items():
		if 'labels' in v:
			scores = []
			for pairs in v['labels']:
				scores.append([ labels_map[pairs[0]][2], pairs[1] ])
			if scores:
				sc.append(json.dumps({k : scores}))

	# print ("\n".join([el for el in sc]))
	open(d + file_type + "_" + "_".join([str(num_pages[0]), str(num_pages[1])]) + '.json', 'w').write("\n".join([el for el in sc]))

# print("\n".join([json.dumps( {k : [ [ labels_map[pairs[0]][2], pairs[1]] for pairs in v['occurrence_map'] if 'occurrence_map' in v ] } ) for k, v in docs_occurrence.items()]))
# {"tweet_id": {"preprocessed": ["bla", "bla", ...], "occurrence_map": [ ["topic1",score], ["topc2", score], ...] }}


