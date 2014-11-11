from concurrent import futures
import sys
sys.path.append(".")
sys.path.append("/opt/texpp")
from outputVerbalPhrase import * 
from concept_occurrence import *

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
	if debug:
		print "Have", len(results), "Sentence matches"
		print "For concepts:", concepts_to_find[0], 'and', concepts_to_find[1]
	for el in results:
		print el[0][1].getTextOfNotTagOnly('N')
	return results

def get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug):
	parse_trees_path = BASE_DIR + '/../../data/parse_trees/'

	if file_type == 'twitter':
		id_parse_tree = {}
		# TODO: Add futures
		for row in sys.stdin:
			v = json.loads(row)
			k = v['id_str']
			text = v['text']
			if not os.path.exists(parse_trees_path + k + '.parse_tree') or os.stat(parse_trees_path + k + '.parse_tree').st_size > 0:
				parse_file(parse_trees_path + k, text, parser_path)
			id_parse_tree[k] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[k] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

	elif file_type == 'news':
		id_parse_tree = {}
		# articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size=100", "http://146.148.70.53/documents/", (0, num_pages))
		import json
		articles = json.loads( open('art.json', 'r').read() )
		# TODO: Add futures
		for k, v in articles.items():
			text = v['title'] + '\n' + v['body']
			if not os.path.exists(parse_trees_path + k + '.parse_tree'):
				if debug:
					print "Processing file with parser"
				parse_file(parse_trees_path + k, text, parser_path)
			id_parse_tree[str(k)] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[str(k)] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

	elif file_type == 'scientific':
		id_parse_tree = {}
		articles = articles_to_map("http://146.148.70.53/documents/list/?type=scientific&full_text=1&page_size=100", "http://146.148.70.53/documents/", (0, num_pages))
		# TODO: Add futures
		for k, v in articles.items():
			text = v['title'] + '\n' + v['body']
			if not os.path.exists(parse_trees_path + k + '.parse_tree'):
				parse_file(parse_trees_path + k, text, parser_path)
			id_parse_tree[str(k)] = parse_trees_path + k + '.parse_tree'
			# id_parse_tree[str(k)] = open(parse_trees_path + k + '.parse_tree', 'r').read()
		return id_parse_tree

	# futures_list = []
	# results = []
	# with futures.ProcessPoolExecutor(max_workers=int(num_threads)) as executor:
	#	 if debug:
	#		 print ">>> Executor started."
	#	 # parse_tree_construction = [pos1, pos2, parse_tree],
	#	 # where pos are the positions of the sentence in the originaltext
	#	 for parse_tree_construction in parse_trees:
	#		 if debug:
	#			 print "processing a parse tree"
	#		 futures_list.append(
	#			 executor.submit(
	#				 find_matched_verbal_phrase, 
	#				 parse_tree_construction, 
	#				 concepts_to_find,
	#				 labels_map,
	#				 debug))
	#		 if debug:
	#			 print "Len of futures_List", len(futures_list)
	#	 for future in futures_list:
	#		 future_result = future.result()
	#		 future_exception = future.exception()
	#		 if future_exception is not None:
	#			 print "!!! Future returned an exception:", future_exception
	#		 else:
	#			 if future_result:
	#				 results.extend(future_result)

if __name__ == "__main__":
	import sys, getopt
	num_threads = 1
	num_pages = 1
	debug = 1
	file_type = 'news'
	file_path = 'articles/s00114-011-0762-7.txt'
	concepta = 'sea level rise'
	conceptb = 'climate change'
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

	id_parse_trees = get_input_ready(file_path, file_type, num_pages, num_threads, parser_path, debug)
	print id_parse_trees
	triplets = parse_triplets(id_parse_trees=id_parse_trees, labels_map=labels_map, concepts_to_find=[concepta, conceptb], parser_path=parser_path, debug=debug)




