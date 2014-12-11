from concurrent import futures
import sys
sys.path.append(".")
sys.path.append("/opt/texpp")
from outputVerbalPhrase import * 
from concept_occurrence import *

import json
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
labels_map = json.loads(open(BASE_DIR + '/../../data/concepts_with_synonyms.json', 'r').read()) # concepts_with_synonyms.concepts_for_topics.json

""" 1. Get an article from input file_path
2. Process each sentence separately
3. Find is there are two concepts that are defined in the sentence
NOTE: Required to texpp library to be imported
4. Output the verbal phrase between the concepts
"""
def parse_triplets(id_parse_trees, labels_map, concepts_to_find=['water', 'drought'], parser_path="/vagrant/stanford-parser-2012-11-12/lexparser.sh", debug=0):
	separator = "_____@@@@@_____"
	results = []
	for k, path_to_parse_trees in id_parse_trees.items():
		parse_trees = parse_tree_from_file(path_to_parse_trees, separator)
		for parse_tree_construction in parse_trees:
			results.extend(
				find_matched_verbal_phrase(parse_tree_construction, 
					concepts_to_find,
					labels_map,
					debug)
				)
	return results

def get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug, smart, labels_map, concepts_to_find):
	from get_parse_tree import parse_fileTextBlob
	parse_trees_path = BASE_DIR + '/../../data/parse_trees/'

	futures_list = []
	if file_type == 'twitter':
		id_parse_tree = {}
		with futures.ProcessPoolExecutor(max_workers=int(num_threads)) as executor:
			for row in open('fts.json', 'r').readlines()[0:2000]:#sys.stdin: #.readlines()[:120]:
				v = json.loads(row)
				k = v['id_str']
				text = v['text']
				if text.startswith('RT'):
					continue
				if not os.path.exists(parse_trees_path + k + '.parse_tree') or os.stat(parse_trees_path + k + '.parse_tree').st_size == 0 or smart == 1:
					futures_list.append(executor.submit(parse_fileTextBlob, parse_trees_path + k, text, parser_path, k, smart, labels_map, concepts_to_find))
				else:
					id_parse_tree[k] = parse_trees_path + k + '.parse_tree'
			for future in futures_list:
				future_result = future.result()
				future_exception = future.exception()
				if future_exception is not None:
					print "!!! Future returned an exception:", future_exception
				else:
					if future_result:
						id_parse_tree[future_result[0]] = future_result[1]
				# id_parse_tree[k] = parse_trees_path + k + '.parse_tree'
		return id_parse_tree

	elif file_type == 'news':
		id_parse_tree = {}
		# articles['id'] = {'title': 'title', 'body' : 'text'}
		article_ids = None
		# article_ids = [row for row sys.stdin]
		articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size=100", "http://146.148.70.53/documents/", num_pages, article_ids)
		# import json
		# articles = json.loads( open('art.json', 'r').read() )
		# TODO: Add futures
		for k, v in articles.items():
			text = v['title'] + '\n' + v['body']
			# if not os.path.exists(parse_trees_path + k + '.parse_tree') or os.stat(parse_trees_path + k + '.parse_tree').st_size == 0:
			if debug:
				print "Processing file with parser"
			parse_fileTextBlob(parse_trees_path + k, text, parser_path, str(k), smart, labels_map, concepts_to_find)
			id_parse_tree[str(k)] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[str(k)] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

	elif file_type == 'scientific':
		id_parse_tree = {}
		articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&full_text=1&page_size=10", "http://146.148.70.53/documents/", num_pages)
		# TODO: Add futures
		for k, v in articles.items():
			text = v['title'] + '\n' + v['body']
			if not os.path.exists(parse_trees_path + k + '.parse_tree') or os.stat(parse_trees_path + k + '.parse_tree').st_size == 0:
				parse_fileTextBlob(parse_trees_path + k, text, parser_path, str(k), smart, labels_map, concepts_to_find)
			id_parse_tree[str(k)] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[str(k)] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

def get_statistics(triplets):
	statistics = {}
	for el in triplets:
		words_between = el[1].getText().split(el[2].getText())[0].rstrip()
		if len(words_between.split(" ")) > 5:
			verbal = el[1].getTextOfNotTagOnly('N')
			if verbal in statistics:
				statistics[verbal] += 1
			else:
				statistics[verbal] = 1
			# print verbal
		else:
			if words_between in statistics:
				statistics[words_between] += 1
			else:
				statistics[words_between] = 1
			# print words_between
	return statistics

def load_existing_predicates(initial_concepts_to_find, file_type):
	import pickle
	predicate_triplets = pickle.loads( 
		open('%s/work/%s-statistics-%s-%s.pickle' % (BASE_DIR, file_type, initial_concepts_to_find[0].replace(' ', '_'), initial_concepts_to_find[1].replace(' ', '_') ),
		'r').read() )
	predicate_map = json.loads(
		open('%s/work/%s-statistics-%s-%s.json' % (BASE_DIR, file_type, initial_concepts_to_find[0].replace(' ', '_'), initial_concepts_to_find[1].replace(' ', '_') ),
		'r').readlines()[0] )
	return predicate_triplets, predicate_map

from scipy.linalg import norm
def simple_cosine_sim(a, b):
    if len(b) < len(a):
        a, b = b, a

    res = 0
    for key, a_value in a.iteritems():
        res += a_value * b.get(key, 0)
    if res == 0:
        return 0

    try:
        res = res / norm(a.values()) / norm(b.values())
    except ZeroDivisionError:
        res = 0
    return res 

def get_computed_statistics_for_s_o(initial_concepts_to_find, file_type):
	# Statistics for the relation predicate
	predicate_triplets, predicate_map = load_existing_predicates(initial_concepts_to_find, file_type)
	statistics_previous = get_statistics([tr[0] for tr in predicate_triplets])
	cleaned_triplets_previous = clean_triplets([tr[0] for tr in predicate_triplets])
	statistics_cleaned_previous = {}
	for tr in cleaned_triplets_previous:
		if tr.final_verbal_phrase in statistics_cleaned_previous:
			statistics_cleaned_previous[tr.final_verbal_phrase] += 1
		else:
			statistics_cleaned_previous[tr.final_verbal_phrase] = 1
	return predicate_triplets, predicate_map, statistics_previous, statistics_cleaned_previous

def get_object_by_subject_predicate(initial_concepts_to_find, id_parse_trees, file_type, labels_map, debug=0):
	separator = "_____@@@@@_____"
	labels_map_topic, hierarchy, topics = read_topic_to_json_from_db(path='http://146.148.70.53/topics/list/?page_size=1000&concepts=1', dir_maps=BASE_DIR+'/../../data/')
	predicate_triplets, predicate_map, statistics_previous, statistics_cleaned_previous = get_computed_statistics_for_s_o(initial_concepts_to_find, file_type)
	subject = initial_concepts_to_find[1]
	objects_for_s_p = []
	for k, path_to_parse_trees in id_parse_trees.items():
		parse_trees = parse_tree_from_file(path_to_parse_trees, separator)
		for parse_tree_construction in parse_trees:
			objects_for_s_p.append(
				find_matched_objects(parse_tree_construction, 
					subject, 
					labels_map,
					statistics_previous, 
					statistics_cleaned_previous,
					debug))
	objects_for_s_p_f_b = {'front':[], 'back':[]}
	if debug:
		print objects_for_s_p
	for f_b in objects_for_s_p:
		objects_for_s_p_f_b['front'].extend(f_b['front'])
		objects_for_s_p_f_b['back'].extend(f_b['back'])
	return objects_for_s_p_f_b


# Required cython
def get_sub_relation_pairs(initial_concepts_to_find, id_parse_trees, file_type ,debug=0):
	labels_map_topic, hierarchy, topics = read_topic_to_json_from_db(path='http://146.148.70.53/topics/list/?page_size=1000&concepts=1', dir_maps=BASE_DIR+'/../../data/')
	separator = "_____@@@@@_____"
	try:
		obj_to_find = sorted([(k,v) for k,v in topics[norm_literal(initial_concepts_to_find[0])].items()], key=lambda x:x[1][0], reverse=True)
	except Exception, e:
		obj_to_find = [(norm_literal(initial_concepts_to_find[0]), [1, initial_concepts_to_find[0]])]

	try:
		subj_to_find = sorted([(k,v) for k,v in topics[norm_literal(initial_concepts_to_find[1])].items()], key=lambda x:x[1][0], reverse=True)
	except Exception, e:
		subj_to_find = [(norm_literal(initial_concepts_to_find[1]), [1, initial_concepts_to_find[1]])]

	# Statistics for the relation predicate
	predicate_triplets, predicate_map, statistics_previous, statistics_cleaned_previous = get_computed_statistics_for_s_o(initial_concepts_to_find, file_type)

	# for each subterms of the Subj and Obj reprectively
	resulting_ranking = []
	for o in obj_to_find[0:10]:
		for s in subj_to_find[0:10]:
			triplets_s_o = []
			for k, path_to_parse_trees in id_parse_trees.items():
				parse_trees = parse_tree_from_file(path_to_parse_trees, separator)
				for parse_tree_construction in parse_trees:
					triplets_s_o.extend(find_matched_verbal_phrase(parse_tree_construction, [s[1][1], o[1][1]], {}, 0))
			statistics = get_statistics([tr[0] for tr in triplets_s_o])
			# print sorted([(k, v) for k, v in statistics.items()], key=lambda x:x[1], reverse=True)
			cleaned_triplets_s_o = clean_triplets([tr[0] for tr in triplets_s_o])
			statistics_cleaned = {}
			for tr in cleaned_triplets_s_o:
				if tr.final_verbal_phrase in statistics_cleaned:
					statistics_cleaned[tr.final_verbal_phrase] += 1
				else:
					statistics_cleaned[tr.final_verbal_phrase] = 1
			# print sorted([(k, v) for k, v in statistics_cleaned.items()], key=lambda x:x[1], reverse=True)

			similarity_before_clean = simple_cosine_sim(statistics_previous, statistics)
			similarity_after_clean = simple_cosine_sim(statistics_cleaned_previous, statistics_cleaned)

			ranking = s[1][0] + o[1][0] + similarity_after_clean
			print (s, o, ranking)
			resulting_ranking.append( (initial_concepts_to_find, s, o, ranking) )
	j = json.dumps(resulting_ranking)
	open('%s/work/subrelations-%s-%s_%s.json' % ( BASE_DIR, file_type, initial_concepts_to_find[0].replace(' ', '_'), initial_concepts_to_find[1].replace(' ', '_') ), 'a').write('\n' + j)

def sort_json_by_occurrence(file_pattern):
	import codecs, json, glob
	for file in glob.glob(file_pattern): #"decrease-*.json"
	    statistics = json.loads(codecs.open(file, 'r', 'utf-8').read())
	    sorted_list = sorted([(k, v) for k, v in statistics.items()], key=lambda x:x[1], reverse=True)
	    codecs.open(file+'.txt', 'w', 'utf-8').write( "\n".join( ["%s_____@@@@@_____%d" % (el[0], el[1]) for el in sorted_list] ) )

if __name__ == "__main__":
	import sys, getopt, codecs, time, pickle
	num_threads = 1
	num_pages = 1
	debug = 0
	# Should we check if there are 2 concepts in the sentence before parsing - then 1
	smart = 0
	file_type = 'news'
	file_path = 'articles/s00114-011-0762-7.txt'
	concepta = 'Climate change'
	conceptb = 'sea level rise'
	parser_path = "/vagrant/stanford-parser-2012-11-12/lexparser.sh"
	argv = sys.argv[1:]
	verbal = False
	subrelations = False
	objects_to_find = False
	try:
		opts, args = getopt.getopt(argv, "hvsot:f:a:b:p:y:n:", ["thread=", "file=", "aconcept=", "bconcept=", "parser=", "file_type=", "num_pages="])
	except getopt.GetoptError:
		print 'outputVerbalPhrase.py -t <number of threads> -f <article/file path> -y <file_type> -n <num_pages ("from,to")> -a <first concepts> -b <second concept> -p <parser file lexparser.sh; default is /vagrant/stanford-parser-2012-11-12/lexparser.sh>'
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print 'outputVerbalPhrase.py -t <number of threads> -f <article/file path> -y <file_type> -n <num_pages ("from,to")> -a <first concepts> -b <second concept> -p <parser file lexparser.sh> -v<verbal phrase to find> -s<subrelation to find>'
			sys.exit()
		elif opt in ("-t", "--thread"):
			num_threads = int(arg)
		elif opt in ("-f", "--file"):
			file_path = arg
		elif opt in ("-y", "--file_type"):
			file_type = arg
		elif opt in ("-v", "--verbal"):
			verbal = True
		elif opt in ("-s", "--subrelations"):
			subrelations = True
		elif opt in ("-o", "--objects_to_find"):
			objects_to_find = True
		elif opt in ("-n", "--num_pages"):
			num_pages = map(int, arg.split(','))
		elif opt in ("-a", "--aconcept"):
			concepta = arg
		elif opt in ("-b", "--bconcept"):
			conceptb = arg
		elif opt in ("-p", "--parser"):
			parser_path = arg
		if file_type == 'news':
			smart = 1

	output_name_pickle = 'work/%s-statistics-%s-%s.pickle' % (file_type, concepta.replace(' ', '_'), conceptb.replace(' ', '_'))
	if verbal:
		if os.path.exists(output_name_pickle):
			triplets = pickle.loads( open(output_name_pickle, 'r').read() )
		else:
			id_parse_trees = get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug, smart, labels_map, [concepta, conceptb])
			triplets = parse_triplets(id_parse_trees=id_parse_trees, labels_map=labels_map, concepts_to_find=[concepta, conceptb], parser_path=parser_path, debug=debug)
		cleaned_triplets = clean_triplets([tr[0] for tr in triplets])
		statistics = get_statistics([tr[0] for tr in triplets])
		statistics_cleaned = {}
		for tr in cleaned_triplets:
			if tr.final_verbal_phrase in statistics_cleaned:
				statistics_cleaned[tr.final_verbal_phrase] += 1
			else:
				statistics_cleaned[tr.final_verbal_phrase] = 1

		output_name = 'work/%s-statistics-%s-%s.json' % (file_type, concepta.replace(' ', '_'), conceptb.replace(' ', '_'))
		timestr = time.strftime("%Y%m%d_%H")
		codecs.open(output_name, 'a', 'utf-8').write("\n" + timestr + "\n")
		codecs.open(output_name, 'a', 'utf-8').write(json.dumps(statistics))
		open(output_name_pickle, 'w'). write( pickle.dumps(triplets) )
		
		print ">>>>>>>>>>>>>>>>>>>>>>>>"
		print "For concepts:", concepta, 'and', conceptb
		print sorted([(k, v) for k, v in statistics_cleaned.items()], key=lambda x:x[1], reverse=True)
	# Required Subj, Obj to be provided. Together with file_type 
	if subrelations:
		id_parse_trees = get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug, smart, labels_map, [concepta, conceptb])
		get_sub_relation_pairs([concepta, conceptb], id_parse_trees, file_type ,debug=0)
	if objects_to_find:
		id_parse_trees = get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug, smart, labels_map, [concepta, conceptb])
		front_back = get_object_by_subject_predicate([concepta, conceptb], id_parse_trees, file_type, labels_map, debug)
		from collections import Counter
		print "Front"
		print Counter(front_back['front'])
		print "Back"
		print Counter(front_back['back'])

