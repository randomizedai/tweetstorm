#!/usr/bin/env python
import os, sys, json, getopt, codecs
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(BASE_DIR)
from concept_occurrence import *

def jaccard(t1, t2):
	from sets import Set
	t1_keys = t1.keys()
	t2_keys = t2.keys()
	intersect = Set(t1_keys).intersection(Set(t2_keys))
	intersection_score = sum([t1[el] + t2[el] for el in list(intersect)])
	disjunction_score = sum(t1.values()) + sum(t2.values())
	return float(intersection_score) / disjunction_score
"""
[topic_name : {'norm_name': topic_norm_name; "concepts": [(concept_i, concept_norm_name_i, weight_i)] }]
"""
def similarity_between_vectors(topic1, topic2):
	t1 = {}
	for v in topic1.values():
		t1[v['norm_name']] = 1
		for el in v['concepts']:
			t1[el[1]] = el[2]
	t2 = {}
	for v in topic2.values():
		t2[v['norm_name']] = 1
		for el in v['concepts']:
			t2[el[1]] = el[2]

	return jaccard(t1, t2)

def similarity_between_each_vectors(topics):
	res = []
	for i, topic1 in enumerate(topics):
		for j, topic2 in enumerate(topics):
			if i < j:
				sim = similarity_between_vectors(topic1, topic2)
				if sim > 0:
					res.append((topic1.keys(), topic2.keys(), sim))
	return sorted(res, key = lambda x:x[2], reverse=True)

if __name__ == "__main__":
	topics = []
	# for row in sys.stdin:
	# 	try:
	# 		topic = json.loads(row)
	# 	except Exception, e:
	# 		continue
	# 	topics.append(topic)

	# topics here are: {topic_norm_name: {con_norm_name:[weight, con_name]}}
	map_, _, top = read_topic_to_json_from_db(path='http://146.148.70.53/topics/list/?page_size=1000&concepts=1', dir_maps=BASE_DIR+'/../../data/')
	for k, v in top.items():
		topic_name = map_[k][0]
		topic = {}
		topic[topic_name] = {}
		topic[topic_name]['norm_name'] = k
		topic[topic_name]['concepts'] = []
		for k_sub, v_sub in v.items():
			topic[topic_name]['concepts'].append((v_sub[1], k_sub, v_sub[0]))
		if topic[topic_name]['concepts']:
			topics.append(topic)

	similarity_scores = similarity_between_each_vectors(topics)
	print similarity_scores[0:10]





