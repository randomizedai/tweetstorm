import sys
sys.path.append(".")
sys.path.append("/opt/texpp")
from article_to_issue import *
from outputVerbalPhrase import * 

class ConceptOccurrence:
	def __init__(self, text, doc_type):
		self.title = ""
		self.abstract = ""
		self.text = text
		self.preprocessed = ""		
		self.doc_type = doc_type
		self.occurrence_map = {}

	def __repr__(self):
		str_ = ';'.join(map(str, [(k, v) for k, v in self.occurrence_map.items()]))
		return "%s\n%s\n%s" % (self.text.encode('utf-8'), self.preprocessed.encode('utf-8'), str_)

	def struct_to_map(self):
		res = {}
		if self.occurrence_map:
			res['preprocessed'] = self.preprocessed
			res['occurrence_map'] = sorted([(k, v) for k,v in self.occurrence_map.items()], key=lambda x:x[1], reverse=True)
		return res

	def update_text_with_underscores(self, tag_tuple_list, general_concepts_map):
		dict_to_check = dict([(l.encode('utf-8'), 1) for l in general_concepts_map.keys()])
		tag_list = get_terms_from_string(self.text, dict_to_check)
		tag_tuple_list_general = [(l.value, l.start, l.end) for l in tag_list]
		if self.title:
			tag_list_title = get_terms_from_string(self.title, dict_to_check)
			for l in tag_list_title:
				tag_tuple_list_general.append((l.value, l.start, l.end))
		self.preprocessed = [el[0] for el in tag_tuple_list_general]
		for el in tag_tuple_list:
			if el[0] not in self.preprocessed:
				self.preprocessed.append(el[0])
		# sorted_tag_list = sorted(tag_tuple_list, key=lambda x: x[1])
		# for i, el in enumerate(sorted_tag_list):
		# 	index = 0 if i == 0 else sorted_tag_list[i-1][2]
		# 	self.preprocessed += self.text[index:el[1]].lower()
		# 	self.preprocessed += "_".join(self.text[el[1]:el[2]].split(' ')).lower()
		# self.preprocessed += self.text[sorted_tag_list[i][2]:].lower()

			
	"""
	labels_map is a map with terms and their synonyms
	labels_map = {norm_name: (norm, norm_name_of_main_concept)}
	"""
	def process_text_with_occurrence(self, text, labels_map, general_concepts_map, multiplier=1, include_hash=1):
		dict_to_check = dict([(l.encode('utf-8'), 1) for l in labels_map.keys()])
		tag_list = get_terms_from_string(text, dict_to_check)
		tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]
		tag_tuple_list_syn = []
		for el in tag_tuple_list:
			tag_tuple_list_syn.append( (labels_map[el[0]][1], el[1], el[2]) )
		self.update_text_with_underscores(tag_tuple_list, general_concepts_map)
		if len(tag_list) == 0:
			return None
		for el in tag_tuple_list_syn:
			if el[0] in self.occurrence_map:
				self.occurrence_map[el[0]] += 1 * multiplier
			else:
				self.occurrence_map[el[0]] = 1 * multiplier
			if include_hash:
				if el[1] > 0 and (text[el[1]-1] == "#" or text[el[1]-1] == "@"):
					self.occurrence_map[el[0]] += 1


	"""
	Result as occurence of termsin labels_map
	plus update to the concepts in the dependency_tree
	Important: Hierarchy is a list where dependent elements should be places further:
	if a ->b,c,d and d -> e,g, then hierarchy should be [d ->e,g; a ->d,...]
	"""
	def get_occurrence_count(self, labels_map, hierarchy, general_concepts_map):
		self.process_text_with_occurrence(self.text, labels_map, general_concepts_map, multiplier = 1)
		if self.title:
			self.process_text_with_occurrence(self.title, labels_map, general_concepts_map, multiplier = 2)
		if self.abstract:
			self.process_text_with_occurrence(self.abstract, labels_map, general_concepts_map, multiplier = 1)
		if self.occurrence_map is not None:
			for kv in hierarchy:
				for v in kv.values()[0]:
					if v in self.occurrence_map.keys():
						if kv.keys()[0] in self.occurrence_map.keys():
							self.occurrence_map[kv.keys()[0]] += self.occurrence_map[v]
						else:
							self.occurrence_map[kv.keys()[0]] = self.occurrence_map[v]


def articles_to_map(path_list, path, pages=[]):
	articles = {}
	next = path_list
	counter = 0
	if len(pages) == 0:
		pages = [0, json.load(urllib2.urlopen(next))["count"]]
	while next and counter < pages[1]:
		try:
			page = json.load(urllib2.urlopen(next))
			for p in page['results']:
				if counter >= pages[0]:
					doc_text = json.load( urllib2.urlopen( path + str(p['id']) ) )['plain_text']
					articles[p['id']] = {'title': p['title'], 'body' : doc_text}
				counter += 1
		except Exception, e:
			return articles
		next = page['next']
	return articles

def tweets_to_map(path_list, path, pages=None):
	tweets = {}
	next = path_list
	counter = 0
	if pages is None:
		pages = json.load(urllib2.urlopen(next))["count"]
	while next and counter < pages:
		if counter % 100 == 0:
			print "read 100 more pages"
		try:
			page = json.load(urllib2.urlopen(next))
		except Exception, e:
			print e
			return tweets
		for p in page['results']:
			doc_text = p['text']
			tweets[p['tweet_id']] = {'text' : doc_text}
		next = page['next']
		counter += 1
	return tweets


def load_csv_terms(path):
	import csv
	f = open(path, 'r')
	data = csv.reader(f, delimiter=',')
	d = [row[0]  for row in data]
	terms_map = {}
	for el in d[1:]:
		norm = norm_literal(el)
		if norm not in terms_map:
			terms_map[norm] = el
	return terms_map

def llda_learn(output_folder, corpus, labels, semisupervised=False):
	import os, codecs
	from tempfile import NamedTemporaryFile
	""" Takes the processed text corpus, labels for each document, and writes
	 this data set to a file for use by the Stanford TMT LLDA scripts.

	Inputs:
	corpus: processed text corpus using other functions
	labels: list of labels for each text document
	fname: file name of the written data set
	semisupervised: True if we want to include unlabeled text documents in the
	data set, False otherwise.

	Output:
	none
	"""
	if semisupervised:
		#TODO: change to the '#'.join(...)
		all_labels = ' '.join(set(reduce(list.__add__, labels)))

	fh = NamedTemporaryFile(delete=False)
	filename = 'input.txt' #fh.name
	fh.close()
	with codecs.open(filename, 'w', 'utf-8') as f:
		for i, label in enumerate(labels):
			# Write label(s) for each document
			if label:
				# TODO: delete quotes
				# text = '\"' + ' '.join(corpus[i][1]) + '\"'
				text = ' '.join(corpus[i][1])
				# TODO: change to '#'.join([...])
				# labels_str = ' '.join(label)
				labels_str = '#'.join(label)
				if labels_str:
					line = ' '.join([str(corpus[i][0]), labels_str, text]) + '\n'
					f.write(line)
			elif semisupervised:
				# delete quotes
				text = '\"' + ' '.join(corpus[i][1]) + '\"'
				labels_str = all_labels
				line = ','.join([str(corpus[i][0]), labels_str, text]) + '\n'
				f.write(line)
			else:
				pass
		f.flush()
		f.seek(0)
		""" Runs LLDA learning/training script on the data set (corpus + labels).
		Inputs:
		tmt: file path of the Stanford TMT jar file
		script: file path of the LLDA scala script
		output_folder: file path of the folder to save the trained model results
		Outputs:
		none
		"""
		import subprocess
		# Delete existing output folder, if applicable
		# TODO: change according to the output
		# remove_folder = ['rm', '-r', output_folder]
		# try:
		# 	subprocess.call(remove_folder)
		# except Exception, e:
		# 	print e
		# Run (L-)LDA script
		# TODO: change the command
		jar_files = output_folder + '/mallet-2.0.7/' + 'dist/mallet-deps.jar:' + output_folder + '/mallet-2.0.7/' + 'dist/mallet.jar'
		command = ['java', '-cp', jar_files, 'cc.mallet.topics.LLDA', fh.name, output_folder+'/mallet-2.0.7/label_index.txt']
		print command
		subprocess.call(command)
	os.unlink(filename)

def wrap_llda(docs_occurrence):
	import os
	BASE_DIR_UTIL = os.path.dirname(os.path.realpath(__file__))  #"/vagrant/julia/VerbalPhraseDetection/tweetstorm/features/utils"
	model_path = BASE_DIR_UTIL # + '/work/llda_model/'
	if not os.path.exists(model_path):
		os.makedirs(model_path)
	# Write data set (corpus, labels) of labeled documents to file
	corpus = [(k, v['preprocessed']) for k, v in docs_occurrence.items() if v]
	labels = [[ el[0]for el in v['occurrence_map']] for k, v in docs_occurrence.items() if v]
	# Run Stanford TMT LLDA training and store results in model_path
	llda_learn(model_path, corpus, labels, semisupervised=False)
	return model_path

def read_topic_vectors(model_path, general_concepts_map, labels_map, file_type=""):
	import csv
	topic_map = {}
	with open(model_path + "01500/summary.txt", 'r') as f:
		data = csv.reader(f, delimiter='\t')
		current_topic = ""
		current_score = 1
		rows = [row for row in data]
		del data
		for el in rows:
			if len(el) > 0:
				if el[0]:
					current_topic = labels_map[el[0]][0]
					topic_map[current_topic] = {"concepts": [], "source" : file_type, 'norm_name': el[0]}
					current_score = float(el[2])
				else:
					score = float(el[2]) / current_score if current_score > 0 else 0
					if el[1] in general_concepts_map:
						element = general_concepts_map[el[1]]
						topic_map[current_topic]["concepts"].append( [element, el[1], score] )
					elif el[1] in labels_map:
						element = labels_map[el[1]][0]
						topic_map[current_topic]["concepts"].append( [element, el[1], score] )
	return topic_map

def rank_element_to_topics(model_path, labels_map, docs_occurrence, tweets_map={}):
	import csv
	labels = {}
	docs = {}
	with open(model_path + "01500/label-index.txt", 'r') as f_labels:
		counter = 0
		for el in f_labels.readlines():
			if el:
				labels[counter] = labels_map[el.strip()][0]
				counter += 1
	with open(model_path + "document-topic-distributions.csv", 'r') as f:
		data = csv.reader(f, delimiter=',')
		rows = [row for row in data]
		del data
	for row in rows:
		if row:
			tweet_id = str(row[0])
			if tweets_map:
				tweet_text =  tweets_map[row[0]]
			else:
				tweet_text = ""
			if tweet_id not in docs_occurrence:
				print tweet_id
			docs[ tweet_id ] = { 'text': tweet_text, 'preprocesed' : docs_occurrence[tweet_id]["preprocessed"], 'labels_map' : docs_occurrence[tweet_id]["occurrence_map"], 'topics' : [] }
			for i in xrange(1, len(row) - 1, 2):
				docs[ tweet_id  ]['topics'].append( [labels[int(row[i])], norm_literal(labels[int(row[i])]), row[i + 1]] )
	return docs
