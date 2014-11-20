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
	parse_trees_path = BASE_DIR + '/../../data/parse_trees/'

	futures_list = []
	results = []
	if file_type == 'twitter':
		id_parse_tree = {}
		with futures.ProcessPoolExecutor(max_workers=int(num_threads)) as executor:
			for row in sys.stdin: #.readlines()[:120]:
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
		articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size=100", "http://146.148.70.53/documents/", (0, num_pages))
		# import json
		# articles = json.loads( open('art.json', 'r').read() )
		# TODO: Add futures
		for k, v in articles.items():
			text = v['title'] + '\n' + v['body']
			if not os.path.exists(parse_trees_path + k + '.parse_tree') or os.stat(parse_trees_path + k + '.parse_tree').st_size == 0:
				if debug:
					print "Processing file with parser"
				parse_fileTextBlob(parse_trees_path + k, text, parser_path, str(k), smart, labels_map, concepts_to_find)
			id_parse_tree[str(k)] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[str(k)] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

	elif file_type == 'scientific':
		id_parse_tree = {}
		articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&full_text=1&page_size=10", "http://146.148.70.53/documents/", (0, num_pages))
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
		words_between = el[0][1].getText().split(el[0][2].getText())[0].rstrip()
		if len(words_between.split(" ")) > 5:
			verbal = el[0][1].getTextOfNotTagOnly('N')
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

def sort_json_by_occurrence(file_pattern):
	import codecs, json, glob
	for file in glob.glob(file_pattern): #"decrease-*.json"
	    statistics = json.loads(codecs.open(file, 'r', 'utf-8').read())
	    sorted_list = sorted([(k, v) for k, v in statistics.items()], key=lambda x:x[1], reverse=True)
	    codecs.open(file+'.txt', 'w', 'utf-8').write( "\n".join( ["%s_____@@@@@_____%d" % (el[0], el[1]) for el in sorted_list] ) )

if __name__ == "__main__":
	import sys, getopt, codecs
	num_threads = 1
	num_pages = 1
	debug = 0
	# Should we check if there are 2 concepts in the sentence before parsing - then 1
	smart = 0
	file_type = 'news'
	file_path = 'articles/s00114-011-0762-7.txt'
	concepta = 'climate change'
	conceptb = 'sea level rise'
	parser_path = "/vagrant/stanford-parser-2012-11-12/lexparser.sh"
	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv, "ht:f:a:b:p:y:n:", ["thread=", "file=", "aconcept=", "bconcept=", "parser=", "file_type=", "num_pages="])
	except getopt.GetoptError:
		print 'outputVerbalPhrase.py -t <number of threads> -f <article/file path> -y <file_type> -n <num_pages> -a <first concepts> -b <second concept> -p <parser file lexparser.sh; default is /vagrant/stanford-parser-2012-11-12/lexparser.sh>'
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			print 'outputVerbalPhrase.py -t <number of threads> -f <article/file path> -y <file_type> -n <num_pages> -a <first concepts> -b <second concept> -p <parser file lexparser.sh>'
			sys.exit()
		elif opt in ("-t", "--thread"):
			num_threads = int(arg)
		elif opt in ("-f", "--file"):
			file_path = arg
		elif opt in ("-y", "--file_type"):
			file_type = arg
		elif opt in ("-n", "--num_pages"):
			num_pages = int(arg)
		elif opt in ("-a", "--aconcept"):
			concepta = arg
		elif opt in ("-b", "--bconcept"):
			conceptb = arg
		elif opt in ("-p", "--parser"):
			parser_path = arg

	id_parse_trees = get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug, smart, labels_map, [concepta, conceptb])
	triplets = parse_triplets(id_parse_trees=id_parse_trees, labels_map=labels_map, concepts_to_find=[concepta, conceptb], parser_path=parser_path, debug=debug)
	if debug:
		print ">>>>>>>>>>>>>>>>>>>>>>>>"
		print "Have", len(triplets), "Sentence matches"
		print "For concepts:", concepts_to_find[0], 'and', concepts_to_find[1]
	statistics = get_statistics(triplets)
	output_name = 'work/%s-statistics-%s-%s.json' % (file_type, concepta.replace(' ', '_'), conceptb.replace(' ', '_'))
	codecs.open(output_name, 'w', 'utf-8').write(json.dumps(statistics))
	print sorted([(k, v) for k, v in statistics.items()], key=lambda x:x[1], reverse=True)
