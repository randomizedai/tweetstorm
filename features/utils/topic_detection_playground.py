import csv, math, codecs, json, urllib2
df  ={}   
with codecs.open('../../data/65k_literals_DFs.xlsx - Sheet1.csv', 'rb', 'utf-8') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        try:
            df[norm_literal(row[0])] = int(row[1])
        except Exception, e:
            continue

import sys
sys.path.append(".")
sys.path.append("/opt/texpp")
from outputVerbalPhrase import * 
from concept_occurrence import *
import get_verbal_phrase

article_ids = [3639837,3639627,3929509,3639897,4002683,3639663,3639691,4002614,3935057,4002712,3972686,4002731,5052125,3635509,3374688,3364947,3364979]

articles = articles_to_map("http://146.148.70.53/documents/list/?type=web&full_text=1&page_size=100", "http://146.148.70.53/documents/", [0,1], article_ids)

f=open('../../data/final_topic_vectors/pr__0.1__sea_level_rise', 'r').readlines()
top = {}
divide_by = 1
for i, el in enumerate(f[0:100]):
    con = el.split(' ')[1].replace('_', ' ')
    weight = float(el.split(' ')[2])
    if i == 0:
        divide_by = weight
        if divide_by == 0:
            divide_by = 1
    if norm_literal(con) not in top:
        top[norm_literal(con)] = [weight / divide_by , con ]
sea_level_topic = top

arts = {}
for k, v in articles.items():
    aaa = ConceptOccurrence(v['body'], 'news')
    aaa.title = v['title']
    terms_from_title = aaa.update_text_with_underscores([], sea_level_topic, output_title=1)
    arts[k] = (aaa, terms_from_title)

from collections import Counter
# TF
n = 1427302
tfidfs = {}
for key, el in arts.items():
    tf = Counter(el[0].preprocessed); tfidf = {}
    for k ,v in tf.items():
        if k in df:
            tfidf[k] = float(v * math.log(n / (1 + df[k]) ) )
        else:
            tfidf[k] = float(v * math.log(n) )
    tfidfs[key] = tfidf

# normalize tfidf
tfidfs_norm = {}
for key, el in tfidfs.items():
    # Occurrences in a title
    nor = {}
    for k, v in el.items():
        nor[k] = float(v / max(el.values()))
        if k in arts[key][1]:
            nor[k] = 1.0
    tfidfs_norm[key] = nor

# multiply for the topic weight
# sea_level_topic
tfidf_norm_topic = {}
for key, el in tfidfs_norm.items():
    tfidf_norm_topic[key] = sum([float(v)*float(sea_level_topic[k][0]) for k, v in el.items()])  # / sum([el[0] for el in sea_level_topic.values()])
# for key,el in tfidf_norm_topic.items():
#     print articles[key]['title']
#     print el


arts_scored = []
for k, v in tfidf_norm_topic.items():
    arts_scored.append((k, v))
sorted_arts_scored = sorted(arts_scored, key=lambda x:x[1], reverse=True)
for el in sorted_arts_scored:
    print "---"
    print el[0]
    print articles[el[0]]['title']
    print el[1]
    print "\n".join([str(v) + "-" + sea_level_topic[k][1]  for k, v in tfidfs[str(el[0])].items()])
    print "...."
    print "\n".join([str(v) + "-" + sea_level_topic[k][1]  for k, v in tfidfs_norm[str(el[0])].items()])

