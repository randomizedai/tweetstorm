#!/usr/bin/env python
import os, sys, json, getopt, codecs
# python xxx.py -y 'tweet' < tweets_jsons.txt > output.txt 
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR + '/utils/')
from concept_occurrence import *

argv = sys.argv[1:]
try:
	opts, args = getopt.getopt(argv, "hy:", ["type="])
except getopt.GetoptError:
	print 'topic_vectors_relevance.py -y <type of the file>'
	sys.exit(2)

file_type = 'twitter'
for opt, arg in opts:
	if opt == '-h':
		print 'topic_vectors_relevance.py -y <type of the file [twitter, news, paper]>'
		sys.exit()
	elif opt in ("-y", "--type"):
		file_type = arg

docs_occurrence = {}
# labels_map and hierarchy is used for assigning preliminary topics to the docs
# TODO: be able to read hierarchy in any order
hierarchy = {} #json.loads(open(BASE_DIR + '/../../data/hierarchy_for_topics.json', 'r').read())
labels_map = json.loads(open(BASE_DIR + '/../../data/concepts_with_synonyms.json', 'r').read()) # concepts_with_synonyms.concepts_for_topics.json
# Labels that are used to construct the docs -> terms vectors
# general_concepts_map is additionally enriched with occurrence of the concepts from the labels_map
general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/amitlist.csv') # 1_climate_keyphrases_aggr_filtered_844

if file_type == "twitter":
	tweets = tweets_to_map("http://146.148.70.53/tweets/list/", "http://146.148.70.53/tweets/")
	for row in sys.stdin: # tweets.items(): #open(BASE_DIR + "/../demo.json", 'r').readlines():
		tweet = json.loads(row)
		occurrence = ConceptOccurrence(tweet['text'], file_type)
		occurrence.get_occurrence_count(labels_map, hierarchy, general_concepts_map)
		docs_occurrence[str(tweet['id_str'])] = occurrence.struct_to_map()

	model_path = wrap_llda(docs_occurrence)
	topic_vector_map = read_topic_vectors(model_path, general_concepts_map, labels_map, file_type)
	document_topic_relevance = rank_element_to_topics(model_path, labels_map, docs_occurrence, tweets)

	open(BASE_DIR + '/work/docs_occurrenceAMIT_DEF_LABELS_N.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in docs_occurrence.items()]))
	open(BASE_DIR + '/work/topic_vector_mapAMIT_DEF_LABELS_N.json', 'w').write("\n\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	open(BASE_DIR + '/work/document_topic_relevanceAMIT_DEF_LABELS_N.json', 'w').write("\n\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))
	print("\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	print("\n\n")
	print("\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))

elif file_type == "web":
	articles = articles_to_map("http://146.148.70.53/documents/list/", "http://146.148.70.53/documents/", 10)
	for k, v in articles.items():
		occurrence = ConceptOccurrence(v['body'], file_type)
		occurrence.title = v['title']
		occurrence.get_occurrence_count(labels_map, hierarchy, general_concepts_map)
		docs_occurrence[str(k)] = occurrence.struct_to_map()

	print("\n".join([json.dumps({k:v}) for k, v in docs_occurrence.items()]))

	model_path = wrap_llda(docs_occurrence)
	topic_vector_map = read_topic_vectors(model_path, general_concepts_map, labels_map)
	try:
		document_topic_relevance = rank_element_to_topics(model_path, labels_map, docs_occurrence, {})
	except Exception, e:
		print e

	print("\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	print("\n\n")
	print("\n".join([json.dumps({k:v['topics']}) for k, v in document_topic_relevance.items()]))

elif file_type == "enb":
	pass
elif file_type == "scientific":
	pass
