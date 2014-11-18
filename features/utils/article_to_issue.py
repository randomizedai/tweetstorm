#!/usr/bin/env python
import os, json, sys, urllib2
# NOTE: We need to have get_parse_tree file in the code folder
sys.path.append(".")
sys.path.append("/opt/texpp")
from get_parse_tree import parse_file, preprocessText, detect_sentences
import parsetreenode
from concept_occurrence import *
from outputVerbalPhrase import norm_literal, get_terms_from_string, get_stopwords
from _chrefliterals import WordsDict, findLiterals, TextTag, TextTagList, normLiteral

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
# BASE_DIR = '/vagrant/julia/VerbalPhraseDetection/tweetstorm/features/utils'

# TODO: Should be different defined concpets!!!
# defined_concepts = json.loads(open(BASE_DIR + "/../../data/concepts_with_synonyms.json",'r').read())
# labels_map = json.loads(open('utils/lab.json', 'r').read())
# topics = json.loads(open('utils/top.json', 'r').read())
issues = "http://146.148.70.53/issues/list/?format=json"
weight = json.loads(open(BASE_DIR + "/../../data/issue_relevance_score_weight.json",'r').read())
# preds = json.load(urllib2.urlopen("http://146.148.70.53/issues/predicate/list/?format=json"))

def read_verbal_ontology(path):
    with open(path + 'verb_vectors/vv-cause.csv', 'r') as f:
        cause = f.readlines()
    causes = {}
    for el in cause:
        causes[el.split(',')[0]] = el.split(',')[1]
    with open(path + 'verb_vectors/vv-increase.csv', 'r') as f:
        inc = f.readlines()
    incs = {}
    for el in inc:
        incs[el.split(',')[0]] = el.split(',')[1]
    with open(path + 'verb_vectors/vv-negative.csv', 'r') as f:
        neg = f.readlines()
    negs = {}
    for el in neg:
        negs[el.split(',')[0]] = el.split(',')[1]
    with open(path + 'verb_vectors/vv-not_cause.csv', 'r') as f:
        not_cause = f.readlines()
    not_causes = {}
    for el in not_cause:
        not_causes[el.split(',')[0]] = el.split(',')[1]
    with open(path + 'verb_vectors/vv-positive.csv', 'r') as f:
        pos = f.readlines()
    poss = {}
    for el in pos:
        poss[el.split(',')[0]] = el.split(',')[1]
    with open(path + 'verb_vectors/vv-reduce.csv', 'r') as f:
        red = f.readlines()
    reds = {}
    for el in red:
        reds[el.split(',')[0]] = el.split(',')[1]
    verbs_map = {}
    verbs_map['cause'] = causes.keys()
    verbs_map['increase'] = incs.keys()
    # verbs_map['negative'] = negs.keys()
    verbs_map['not_cause'] = not_causes.keys()
    # verbs_map['positive'] = poss.keys()
    verbs_map['decrease'] = reds.keys()
    return verbs_map

def findWholeWord(w):
    import re
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

def predicate_synonimization(sentence, verbs_map):
    predicate_set = []
    for k, v in verbs_map.iteritems():
        for verbs in v:
            if norm_literal(verbs) in norm_literal(sentence):
            # if findWholeWord(norm_literal(verbs))(norm_literal(sentence)):
                predicate_set.append(k)
                break
    return list(set([l for l in predicate_set]))

def main(file_type, text, concepts_to_find, verbal_map, labels_map, hierarchy, topics):
    import codecs    
    list_to_check = []
    try:
        obj_to_find = [subtopic for subtopic in topics[concepts_to_find[0]].keys()]
        obj_to_find.append(concepts_to_find[0])
    except Exception, e:
        obj_to_find = [concepts_to_find[0]]

    try:
        subj_to_find = [subtopic for subtopic in topics[concepts_to_find[1]].keys()]
        subj_to_find.append(concepts_to_find[1])
    except Exception, e:
        subj_to_find = [concepts_to_find[1]]

    list_to_check = list(set(obj_to_find + subj_to_find))

    dict_to_check = dict([(l.encode('utf-8'), 1) for l in list_to_check])
    indicator = {'occurrence_of_s_or_o': 0.0, 
                'occurrence_of_s_and_o': 0.0,
                'occurrence_of_s_and_p_and_o' : 0.0}

    tag_list = get_terms_from_string(text, dict_to_check)

    # TODO: Might be changed later to check if it is of any of the issues
    obj_representatives = [l.value for l in tag_list if l.value in obj_to_find]
    subj_representatives = [l.value for l in tag_list if l.value in subj_to_find]
    if len(obj_representatives) == 0 or len(subj_representatives) == 0:
    # if len(set([defined_concepts[l.value][1] for l in tag_list if l.value in topics[concepts_to_find[0]].keys() ] )) < 2:
        return indicator

    dot_list = detect_sentences(text)
    tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]

    for el in dot_list:
        tag_tuple_list.append(('.', el, el + 1))
    sorted_tag_list = sorted(tag_tuple_list, key=lambda x: x[1])

    #TODO: Fix the syn into the other list - for a syn have a [list]
    sorted_tag_list_syn = []
    for el in sorted_tag_list:
        if el[0] != ".":
            try:
                if el[0] in concepts_to_find:
                    sorted_tag_list_syn.append(el)
                elif el[0] in obj_representatives:
                    sorted_tag_list_syn.append( (concepts_to_find[0], el[1], el[2], el[0]) )
                else:
                    sorted_tag_list_syn.append( (concepts_to_find[1], el[1], el[2], el[0]) )
                # sorted_tag_list_syn.append( (defined_concepts[el[0]][1], el[1], el[2]) )
            except Exception, e:
                continue
        else:
            sorted_tag_list_syn.append(el)

    if len(sorted_tag_list_syn) == 0 or sorted_tag_list_syn[-1][0] != '.':
        sorted_tag_list_syn.append( ('.', len(text), len(text)) )

    num_of_sentence = 1 #len(dot_list)
    count_with_sentence_with_conceps = 0
    count_with_sentence_with_both_conceps = 0
    count_with_sentence_with_both_conceps_predicate = 0
    num_elems = len(sorted_tag_list_syn)
    i = -1
    while i < len(sorted_tag_list_syn):
        if i < 0 or sorted_tag_list_syn[i][0] == '.':
            k = i + 1
            while k < num_elems and sorted_tag_list_syn[k][0] != '.':
                k += 1
            sen = ""
            # if we are in the middle of the array
            if i < k - 1 and k < num_elems:
                count_hash_mention = 0
                if file_type == "tweet":
                    count_hash_mention_ = [el for el in sorted_tag_list_syn[i+1:k] if (text[el[1]-1] == "#" or text[el[1]-1] == "@")]
                    for el in count_hash_mention_:
                        if len(el) == 3:
                            count_hash_mention += 1
                            continue
                        elif len(el) > 3:
                            if el[3] in obj_representatives:
                                count_hash_mention += topics[concepts_to_find[0]][el[3]][0]
                            else:
                                count_hash_mention += topics[concepts_to_find[1]][el[3]][0]
                    # print "added count for hashes", count_hash_mention
                count_mention_ = [el for el in sorted_tag_list_syn[i+1:k] ]
                if len(count_mention_) < 2:
                    return indicator
                count_with_sentence_with_conceps += k - 1 - i + count_hash_mention
                # print "overall count added", k - 1 - i + count_hash_mention
                if i < k - 2 and len(set([l[0] for l in sorted_tag_list_syn[i+1:k]])) > 1:
                    for el in count_mention_:
                        if len(el) == 3:
                            count_with_sentence_with_both_conceps += 1
                            continue
                        if len(el) > 3:
                            if el[3] in obj_representatives:
                                try:
                                    count_with_sentence_with_both_conceps += topics[concepts_to_find[0]][el[3]][0]
                                except Exception, e:
                                    continue
                            else:
                                try:
                                    count_with_sentence_with_both_conceps += topics[concepts_to_find[1]][el[3]][0]
                                except Exception, e:
                                    continue
                    index = 0 if i < 0 else sorted_tag_list_syn[i][2] + 1
                    sen = text[index:sorted_tag_list_syn[k][2]]
                    predicates = predicate_synonimization(sen, verbal_map)
                    if sum([1 for el in sorted_tag_list_syn[i+1:k] if len(el) == 3]) > 1:
                        for pred in predicates:
                            if pred in concepts_to_find[2]:
                                count_with_sentence_with_both_conceps_predicate += 1
                                break
            i = k

    indicator['occurrence_of_s_or_o'] = float(count_with_sentence_with_conceps) / num_of_sentence
    indicator['occurrence_of_s_and_o'] = float(count_with_sentence_with_both_conceps) / num_of_sentence
    indicator['occurrence_of_s_and_p_and_o'] = float(count_with_sentence_with_both_conceps_predicate) / num_of_sentence

    return indicator

def compute_indicators_inner(file_type, text, title, abstract, id_element, verbal_map, triplets, labels_map, hierarchy, topics):
    total_score = 1 #sum([v for k, v in weight.iteritems()])
    res_ = {}
    res_[id_element] = []
    for key, value in triplets.items():
        indicator_body = main(file_type = file_type, text=text, concepts_to_find=value, verbal_map=verbal_map, labels_map=labels_map, hierarchy=hierarchy, topics=topics)
        index_body = sum([weight[k] * v for k, v in indicator_body.iteritems()]) / total_score
        index_title = 0.0
        index_abstract = 0.0
        if title:
            indicator_title = main(file_type = file_type, text=title, concepts_to_find=value, verbal_map=verbal_map, labels_map=labels_map, hierarchy=hierarchy, topics=topics)
            index_title = sum([weight[k] * v for k, v in indicator_title.iteritems()]) / total_score
        if abstract:
            indicator_abstract = main(file_type = file_type, text=abstract, concepts_to_find=value, verbal_map=verbal_map, labels_map=labels_map, hierarchy=hierarchy, topics=topics)
            index_abstract = sum([weight[k] * v for k, v in indicator_abstract.iteritems()]) / total_score
        index = weight['index_body'] * index_body \
              + weight['index_title'] * index_title \
              + weight['index_abstract'] * index_abstract
        if index > 0:
            res_[id_element].append([key, index])
    if res_[id_element]:
        return res_

# Output: issue_id : [obj_norm_name; subj_norm_name; predicate_name; obj; subj]
def issues_to_map(path):
    triplets = {}
    next = path
    while next:
        issues = json.load(urllib2.urlopen(next))
        for issue in issues['results']:
            triplets[issue['id']] = [norm_literal(issue['object']['name']), \
                                        norm_literal(issue['subject']['name']), \
                                        issue['predicate']['name'], \
                                        issue['object']['name'],
                                        issue['subject']['name']]
        next = issues['next']
    return triplets

        
def get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map, triplets, labels_map, hierarchy, topics):
    import json
    res_ = {}
    tweet_id_text = {}
    if file_type == "tweet":
        if text == None:
            return res_ 
        tweet = json.loads(text)
        text = tweet['text']
        id_element = tweet['id_str']
    else: #elif file_type == "news" or file_type == 'paper':
        if text == None:
            text = codecs.open(file_path, 'r', 'utf-8').read()
        id_element = file_path

    # TODO: Check the formal of title and abstract and pass them as text to the function
    res_ = compute_indicators_inner( 
        file_type=file_type, 
        text=text, 
        title=title, 
        abstract=abstract, 
        id_element=id_element, 
        verbal_map=verbal_map, 
        triplets=triplets,
        labels_map=labels_map, 
        hierarchy=hierarchy, 
        topics=topics)

    # json_output = json.dumps(res_, indent = 4)
    # # Add time to the file name
    # utc_datetime = datetime.datetime.utcnow()
    # formated_string = utc_datetime.strftime("%Y-%m-%d-%H%MZ")
    # file_name = os.path.basename(os.path.realpath(file_path))
    # filename = '%s_%s_%s.json'% (file_type, file_name, formated_string)
    # f = codecs.open(filename, 'w')
    # f.write(json_output)
    # f.close()
    return res_

def rank_resulting_map(result):
    issue_ranking = {}
    for issue_pair in triplets.keys():
        list_ = []
        for k, v in result.items():
            for el in v:
                if el[0] == issue_pair:
                    list_.append([k, el[1]])
        sorted_list = sorted(list_, key = lambda x : x[1], reverse = True)
        issue_ranking[issue_pair] = sorted_list

    for k, v in issue_ranking.items():
        print k
        print '[%s]' % '\n '.join(map(str, [(vv[0], vv[1]) for vv in v if vv[1] > 0][0:10]))

# if __name__ == "__main__":
#     import sys, getopt
#     import json, codecs
#     import datetime

#     # compute relevant to the number of sentence
#     argv = sys.argv[1:]
#     try:
#         opts, args = getopt.getopt(argv, "hf:y:e:i:a:v:", ["file=", "text=", "type=", "title=", "abstract=", "verbalmappath="])
#     except getopt.GetoptError:
#         print 'article_to_issue.py -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any>'
#         sys.exit(2)

#     file_type = "tweet"
#     title = None
#     text = None
#     abstract = None
#     file_path = "TwitterData_100Kjson.json"
#     verbal_path = ""
#     for opt, arg in opts:
#         if opt == '-h':
#             print 'article_to_issue.p -f <article/file path> -y <type of the file> -i <title if any> -a <abstract if any> -v <verbal path unless in current directory>'
#             sys.exit()
#         elif opt in ("-y", "--type"):
#             file_type = arg
#         elif opt in ("-f", "--file"):
#             file_path = arg
#         elif opt in ("-i", "--title"):
#             title = arg
#         elif opt in ("-a", "--abstract"):
#             abstract = arg
#         elif opt in ("-e", "--text"):
#             text = arg
#         elif opt in ("v", "--verbalmappath"):
#             verbal_path = arg

#     verbal_map = read_verbal_ontology(verbal_path)
#     # pairs = {(a, b): (a_norm, b_norm)}
#     result = {}
#     if file_type == 'tweet':
#         for line in open(file_path, 'r').readlines()[0:100]:
#             result.update(get_indicator_body_title_abstact(file_path, file_type, line, title, abstract, verbal_map))
#     else:
#         result.update(get_indicator_body_title_abstact(file_path, file_type, text, title, abstract, verbal_map))
#     # print json.dumps(result)

