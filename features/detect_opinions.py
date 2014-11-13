'''
Created on Nov 13, 2014

@author: gupta
'''
from utils.ahocorasick import *
import os,sys,json,itertools
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils2 import clean_tweet,stem_string
import re
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
data = []
with open(BASE_DIR + "/../data/relevant_concepts.csv") as fp:
  data = fp.readlines()

NAI_list = ["opinion","discussion","debate","question","at odds","facts and myth"]
POI_list = ["deniers","deniers"]
NOI_list = ["alarmists","bullshit","speculative","speculation","myth","megamyth","hysteria","hoax","climate hoax","propaganda","alarmist"]
ignore_negtion_follow = ["choose","just","only","always","choose","wait","get","understand"]

def split_string_into_sentences(string):
    listv = re.split("(\.|\?|!|:|;but)",string)
    num = len(listv) / 2
    mod = len(listv) % 2
    ans = []
    for i in range(0,num):
        ans.append(listv[2*i] + listv[2*i+1])
    if  mod == 1:
        ans.append(listv[-1])    
    ans = [ x.strip() for x in ans if x]    
    return ans

ifo_data = []
cv_data = []
nv_data = []
with open(BASE_DIR + "/../data/issues_for_opinions.csv") as fp:
    ifo_data = fp.readlines()
with open(BASE_DIR + "/../data/concept_vectors_for_opinions.csv") as fp:
    cv_data = fp.readlines()
with open(BASE_DIR + "/../data/negation_verbs.txt") as fp:
    nv_data = fp.readlines()

def lower_string(strv):
    return "_".join(strv.lower().strip().split())    

def convert_string_to_hashtag(string):
    return "#" + "".join(string.split()) 

def getComplexTagger(listv):
    tokens = [x for x in listv if len(x.strip()) > 0]
    two_word_tokens = [x for x in tokens if len(x.split()) == 2]
    reverse_two_word_tokens = [" ".join(x.split()[::-1]) for x in two_word_tokens ]
    tokens = tokens + reverse_two_word_tokens
    stemmed_tokens = [stem_string(x) for x in tokens]
    stemmed_hash_keywords = [convert_string_to_hashtag(x) for x in stemmed_tokens if x]
    unstemmed_hash_keywords = [convert_string_to_hashtag(x) for x in tokens if x]
    keywords = tokens + stemmed_hash_keywords + unstemmed_hash_keywords + stemmed_tokens
    return getTagger(keywords, debug=False)


    
issues_dict = {}
cv_dict = defaultdict(list)
nv_list = []
for line in ifo_data:
    tokens = line.strip().split(",")
    string = "__".join(sorted(tokens[1:3])) # sorted concepts 
    issues_dict[string] = (int(tokens[0]),int(tokens[3]))  # issue_id, opinion_dir

for line in cv_data:
    tokens = line.split(",")
    cv_dict[lower_string(tokens[0])] = tokens

for line in nv_data:
    nv_list.append(line.strip().lower())

#get All Taggers
NAI_tagger = getComplexTagger(NAI_list)
POI_tagger = getComplexTagger(POI_list)
NOI_tagger = getComplexTagger(NOI_list)
NV_tagger =  getTagger(nv_list)
CV_taggers = {}
for k,v in cv_dict.items():
    CV_taggers[k] = getComplexTagger(v)



def sentence_opinions(sentence):
    sentence = sentence.strip().lower().replace("\'","").replace("-"," ")
    tokens = text.strip().split()
    nonhashtag_tokens = [x for x in tokens if x[0] != "#"]
    op_neutral = 0
    op_pos = 0
    ans = []
    # NAI rule     
    op_matches = NAI_tagger.tag(sentence)
    if op_matches or (tokens and (nonhashtag_tokens[0] == "if" or nonhashtag_tokens[-1] == "?")):
        op_neutral = 1
    
    #POI rule
    if "denier" in tokens or "deniers" in tokens:
        op_pos = 1
             
             
    #Negation
    neg_verbs_matches = NV_tagger.tag(text)
    neg_verbs_matches_new = []
    for d in neg_verbs_matches:
        l = d['endpos']
        if (l+1) < len(tokens) and tokens[l+1]  in ignore_negtion_follow:
            continue
        neg_verbs_matches_new.append(d)          
        
    #NOI rule
    neg_words_matches = NOI_tagger.tag(text) 
    
    
    #Detect Concepts
    matches = []
    for concept,tagger in CV_taggers.items():
        output1 = tagger.tag(text)
        output = [(concept,x) for x in output1]
        matches += output
    
    concept_pairs = []
    sorted_matches = sorted(matches, key = lambda x : x[1]['beginpos'])
    for i in range(0, len(sorted_matches)):
        for j in range(i+1,len(sorted_matches)):
            c1 = sorted_matches[i][0]               
            c2 = sorted_matches[j][0]
            issue_string = ""
            if c1 < c2:
                issue_string = c1 + "__" + c2
            else:
                issue_string = c2 + "__" + c1                
            
            if issue_string in issues_dict:
                concept_pairs.append(issue_string)
                   
    for x in list(set(concept_pairs)):
        if op_neutral == 1:
            ans.append([x,0])
        elif op_pos == 0 and ( (len(neg_verbs_matches_new))%2 == 1  or neg_words_matches):
            ans.append([x,-1])
        else:
            ans.append([x,+1]) 

    return ans 


def tweet_opinions(clean_text):
    sentences = split_string_into_sentences(clean_text)
    ans = []
    for sent in sentences:
        ans += sentence_opinions(sent)
    return ans

for row in sys.stdin:
    #try:
        tweet = json.loads(row)
        text = tweet['text']
        clean_text = clean_tweet(text)
        opinions = tweet_opinions(clean_text)
        issue_done = {}
        #if opinions:
        #    print tweet['id_str'] + "," +clean_text
        for k,v in opinions:
            if k not in issue_done:
                issue_id,issue_dir = issues_dict[k]
                print  str(tweet['id_str']) + ","+ str(issue_id) + "," + str(v*issue_dir )
                issue_done[k] = 1    
                
    #except:
    #    pass 