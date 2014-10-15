import sys
sys.path.append(".")
sys.path.append("/opt/texpp")
from article_to_issue import *
from outputVerbalPhrase import * 

BASE_DIR = os.path.dirname(os.path.realpath(__file__))  #"/vagrant/julia/VerbalPhraseDetection/tweetstorm/features/utils"

class ConceptOccurrence:
	def __init__(self, text):
		self.text = text
		self.preprocessed = ""
		self.occurrence_map = None

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
		tag_tuple_list_general.extend(tag_tuple_list)
		self.preprocessed = [el[0] for el in tag_tuple_list_general]
		# sorted_tag_list = sorted(tag_tuple_list, key=lambda x: x[1])
		# for i, el in enumerate(sorted_tag_list):
		# 	index = 0 if i == 0 else sorted_tag_list[i-1][2]
		# 	self.preprocessed += self.text[index:el[1]].lower()
		# 	self.preprocessed += "_".join(self.text[el[1]:el[2]].split(' ')).lower()
		# self.preprocessed += self.text[sorted_tag_list[i][2]:].lower()

			
	"""
	concepts_map is a map with terms and their synonyms
	concepts_map = {norm_name: (norm, norm_name_of_main_concept)}
	"""
	def process_text_with_occurrence(self, concepts_map, general_concepts_map):
		dict_to_check = dict([(l.encode('utf-8'), 1) for l in concepts_map.keys()])
		tag_list = get_terms_from_string(self.text, dict_to_check)
		if len(tag_list) == 0:
			return None
		tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]
		tag_tuple_list_syn = []
		for el in tag_tuple_list:
			tag_tuple_list_syn.append( (concepts_map[el[0]][1], el[1], el[2]) )
		self.update_text_with_underscores(tag_tuple_list_syn, general_concepts_map)
		occurrence_count  = {}
		for el in tag_tuple_list_syn:
			if el[0] in occurrence_count:
				occurrence_count[el[0]] += 1
			else:
				occurrence_count[el[0]] = 1
			if el[1] > 0 and (self.text[el[1]-1] == "#" or self.text[el[1]-1] == "@"):
				occurrence_count[el[0]] += 1
		return occurrence_count


	"""
	Result as occurence of termsin concepts_map
	plus update to the concepts in the dependency_tree
	Important: Hierarchy is a list where dependent elements should be places further:
	if a ->b,c,d and d -> e,g, then hierarchy should be [d ->e,g; a ->d,...]
	"""
	def get_occurrence_count(self, concepts_map, hierarchy, general_concepts_map):
		occurrence_map = self.process_text_with_occurrence(concepts_map, general_concepts_map)
		if occurrence_map is not None:
			for kv in hierarchy:
				for v in kv.values()[0]:
					if v in occurrence_map.keys():
						if kv.keys()[0] in occurrence_map.keys():
							occurrence_map[kv.keys()[0]] += occurrence_map[v]
						else:
							occurrence_map[kv.keys()[0]] = occurrence_map[v]
		self.occurrence_map = occurrence_map

	def rank_element_to_topics(self, concepts_vectors):
		pass

		
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

def llda_learn(tmt, script, output_folder, corpus, labels, semisupervised=False):
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
		all_labels = ' '.join(set(reduce(list.__add__, labels)))

	fh = NamedTemporaryFile(delete=False)
	filename = fh.name
	fh.close()
	with codecs.open(filename, 'w', 'utf-8') as f:
		for i, label in enumerate(labels):
			# Write label(s) for each document
			if label:
				text = '\"' + ' '.join(corpus[i]) + '\"'
				labels_str = ' '.join(label)
				line = ','.join([str(i), labels_str, text]) + '\n'
				f.write(line)
			elif semisupervised:
				text = '\"' + ' '.join(corpus[i]) + '\"'
				labels_str = all_labels
				line = ','.join([str(i), labels_str, text]) + '\n'
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
		remove_folder = ['rm', '-r', output_folder]
		try:
			subprocess.call(remove_folder)
		except Exception, e:
			print e
		# Run (L-)LDA script
		command = ['java', '-jar', tmt, script, fh.name, output_folder]
		print command
		subprocess.call(command)
	os.unlink(filename)

def wrap_llda(docs_occurrence):
	import os
	model_path = BASE_DIR + '/work/llda_model/'
	if not os.path.exists(model_path):
		os.makedirs(model_path)
	llda_learn_script = BASE_DIR + '/6-llda-learn.scala'
	tmt_file = BASE_DIR + '/tmt-0.4.0.jar'
	# Write data set (corpus, labels) of labeled documents to file
	corpus = [v['preprocessed'] for k, v in docs_occurrence.items() if v]
	labels = [[ el[0]for el in v['occurrence_map']] for k, v in docs_occurrence.items() if v]
	# Run Stanford TMT LLDA training and store results in model_path
	llda_learn(tmt_file, llda_learn_script, model_path, corpus, labels, semisupervised=False)
	return model_path

def read_topic_vectors(model_path, general_concepts_map, concepts_map):
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
					current_topic = concepts_map[el[0]][0]
					topic_map[current_topic] = []
					current_score = float(el[2])
				else:
					score = float(el[2]) / current_score
					if el[1] in general_concepts_map:
						element = general_concepts_map[el[1]]
						topic_map[current_topic].append( [element, score] )
					elif el[1] in concepts_map:
						element = concepts_map[el[1]][0]
						topic_map[current_topic].append( [element, score] )
	return topic_map

def rank_element_to_topics(model_path, concepts_map):
	import csv
	labels = {}
	docs = {}
	with open(model_path + "01500/label-index.txt", 'r') as f_labels:
		counter = 0
		for el in f_labels.readlines():
			if el:
				labels[counter] = concepts_map[el.strip()][0]
				counter += 1
	with open(model_path + "document-topic-distributions.csv", 'r') as f:
		data = csv.reader(f, delimiter=',')
		rows = [row for row in data]
		del data
	for row in rows:
		if row:
			docs[row[0]] = []
			for i in xrange(1, len(row) - 1, 2):
				docs[row[0]].append( [labels[int(row[i])], row[i + 1]] )
	return docs


if __name__ == "__main__":
	import json
	docs_occurrence = {}
	hierarchy = json.loads(open(BASE_DIR + '/../../data/hierarchy_for_topics.json', 'r').read())
	concepts_map = json.loads(open(BASE_DIR + '/../../data/concepts_for_topics.json', 'r').read())
	general_concepts_map = load_csv_terms(BASE_DIR + '/../../data/climatelex.csv')  #json.load(urllib2.urlopen("???"))
	for row in sys.stdin: #open(BASE_DIR + "/../demo.json", 'r').readlines():
		tweet = json.loads(row)
		occurrence = ConceptOccurrence(tweet['text'])
		occurrence.get_occurrence_count(concepts_map, hierarchy, general_concepts_map)
		docs_occurrence[tweet['id_str']] = occurrence.struct_to_map()
	
	model_path = wrap_llda(docs_occurrence)
	topic_vector_map = read_topic_vectors(model_path, general_concepts_map, concepts_map)
	document_topic_relevance = rank_element_to_topics(model_path, concepts_map)
	
	open(BASE_DIR + '/work/docs_occurrence.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in docs_occurrence.items()]))
	open(BASE_DIR + '/work/topic_vector_map.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in topic_vector_map.items()]))
	open(BASE_DIR + '/work/document_topic_relevance.json', 'w').write("\n".join([json.dumps({k:v}) for k, v in document_topic_relevance.items()]))

