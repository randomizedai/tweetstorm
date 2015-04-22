import sys
import os
# NOTE: We need to have get_parse_tree file in the code folder
sys.path.append(".")
sys.path.append("/opt/texpp")
import parsetreenode
from _chrefliterals import WordsDict, findLiterals, TextTag, TextTagList, normLiteral

ABBR_MAX = 4
DictWords = WordsDict('/usr/share/dict/words', ABBR_MAX)
DictWords.insert('I')
DictWords.insert('a')
DictWords.insert('sea')
DictWords.insert('bio')
DictWords.insert('coal')
DictWords.insert('land')
DictWords.insert('use')
DictWords.insert('oil')
DictWords.insert('rise')
DictWords.insert('fuel')
DictWords.insert('gas')
DictWords.insert('tax')
DictWords.insert('ice')
DictWords.insert('loss')
DictWords.insert('reef')
DictWords.insert('bear')
DictWords.insert('heat')
DictWords.insert('wave')

knownNotLiterals = dict.fromkeys((
    'i.e.', 'ie.', 
    'c.f.', 'cf.', 'cf',
    'e.g.', 'eg.',
    'de', 'De'  # for de in the names of Universities
))

# Get the list of parse tree for the sentence from the file with parse trees
# RETURS: list of sentences from the parse file. 
# [(pos1, pos2, parse_tree), ...]
def parse_tree_from_file(path_to_parse_trees, separator):
    import os
    # open file to read the parse tree for the sentence
    # ONLY READ THE FILE LOCALLY FOR NOW
    text = read_file_remotely_or_localy(path_to_parse_trees).rstrip().lstrip()
    # Split the text by the file path
    # dirname = os.path.dirname(os.path.realpath(path_to_parse_trees))
    sentences = text.split('\n')
    # split the lines by the separator
    sen_parse_tree_list = []
    positions_map = []
    for sen in sentences:
        if sen:
            try:
                # that is what is stored in the parse file
                article_name, positions, sentence, parse_tree = sen.split(separator)
                if positions not in positions_map:
                    positions_map.append(positions)
                else:
                    continue
                position_current_1, position_current_2 = positions.split("_")
                position_current_1 = int(position_current_1)
                position_current_2 = int(position_current_2)
            except ValueError:
                continue
            sen_parse_tree_list.append((position_current_1, position_current_2, parse_tree))
    sen_parse_tree_list_sorted = sorted(sen_parse_tree_list, key=lambda x: x[0])
    return sen_parse_tree_list_sorted


def get_stopwords():
    #from nltk.corpus import stopwords
    #return dict.fromkeys(stopwords.words('english'))
    return dict()

# SW like function to get terms from the sentence/articles/text among provided list of concepts
# aka datalib ahocorasick thing but from texpp
# should not be used if datalib is used. 
def get_terms_from_string(sentence, literals):
    """
    Extract terms from a given string of text
    :param literals: pre-defined list of literals to search in text
    """
    import re
    literals = literals
    tag_list = TextTagList()
    start = 0
    split_re = re.compile('\w+|\W', flags=re.U)
    non_word = re.compile('^\W$', flags=re.U)

    for tag in split_re.findall(sentence):
        if tag:
            if non_word.match(tag):
                tag_type = TextTag.Type.CHARACTER
            else:
                tag_type = TextTag.Type.WORD

            try:
                tag_list.append(TextTag(tag_type, start, start + len(tag), unicode(tag).encode('utf-8')))
            except Exception, e:
                pass
            start += len(tag)
    literalTags = findLiterals(tag_list, literals, knownNotLiterals,
                               DictWords, get_stopwords(), 0, False)
    return literalTags


# Returns the normalized form of the literal/concept/text
# uses texpp
def norm_literal(literal):
    """Return normalized literal form"""
    literal = str(literal.encode('utf-8', 'ignore'))
    n_literal = normLiteral(literal, DictWords, get_stopwords(), False)
    return n_literal.decode('utf-8', 'ignore')


'''
Find if node1 and node2 are the part of the NpVpNp pattern
Returns:
  Node1
  Predicate which contains Node2 as well
  Node2

First half of the code for this function is simply finds 
    the beginning of the NP that concepts belong to

Second, it is identified which concepts lays in which part of the NPVP structure
    that is, first we assume that pnode2 have a parent who has a sibling pnode1 (pnode1.findMyParentSiblingFor(pnode2))
    if it is true  - this means that pnode2 lays in Np that belong to Vp and that Vp is a sibling for the Np that pnode1 is in.
    else, opposite
    
'''
# TODO: Add functionality: printing the predicate - 
# so that to have printed words only between two concepts with a tag V_
def findNpVpNpPatternFor(node1, node2, debug=0):
    if node1.parent is not None and node1.parent.tag.startswith('N'):
        pnode1 = node1.parent
    else:
        pnode1 = node1
    if node2.parent is not None and node2.parent.tag.startswith('N'):
        pnode2 = node2.parent
    else:
        pnode2 = node2

    if debug:
        print ">>>>>>>> Node names (parent node names)"
        print pnode1.getText()
        print pnode2.getText()

    if not pnode1.tag.startswith('N'):
        return None
    if not pnode2.tag.startswith('N'):
        return None

    sibling1 = pnode1.findMyParentSiblingFor(pnode2)
    if sibling1 is not None:
        if debug:
            print "Sibling found"
            print sibling1.tag
            print sibling1.getText()
        if sibling1.tag.startswith('V'):
            return node2, sibling1, node1
    else:
        sibling2 = pnode2.findMyParentSiblingFor(pnode1)
        if sibling2 is not None:
            if debug:
                print "Sibling found"
                print sibling2.tag
                print sibling2.getText()
            if  sibling2.tag.startswith('V'):
                return node1, sibling2, node2
    return None

# For given list of concepts in the sentence, the function returns
# the list of concepts pairs to be checked on the presentece of relation between
def get_pairs_from_concepts(tag_tuple_list, concepts_to_find):
    concepts_map = {}
    new_tag_tuple_map = {}
    concept_names = list(set([l[0] for l in tag_tuple_list]))
    if len(concept_names) == 2:
        # ensuring relative position of concepts - since their pair might make sense. 
        # like we r looking for climate change and storm, and not reverse. Might be used for some app
        if concept_names[0] != norm_literal(concepts_to_find[0]):
            concept_names = [concept_names[1], concept_names[0]]
        # initializing the maps for occurrence of con1 and con2
        for con in concept_names:
            concepts_map[con] = 0
            new_tag_tuple_map[con] = []
        #
        for el in tag_tuple_list:
            new_tag_tuple_map[el[0]].append((el[0], 
                el[1], 
                el[2], 
                concepts_map[el[0]]))
            # numerating multiple concepts, like we met twice carbon
            concepts_map[el[0]] = concepts_map[el[0]] + 1

        # OUTPUTS ONLY PAIRS BETWEEN c1' and c2', where ' means that c1 and its synonyms etc 
        # + adds the relative position of concepts (relative to the initial input positions)
        # like we asked to give pairs between a and b, and if we have 'b .. a' in a sentence we will add 1 to the yield
        for positionA in new_tag_tuple_map[concept_names[0]]:
            for positionB in new_tag_tuple_map[concept_names[1]]:
                if positionA[1] < positionB[1]:
                    yield positionA, positionB, 0
                else:
                    yield positionB, positionA, 1
        

"""
Function to be run in parallel
Input: Given the text of the file, sentence positions and parse tree as parse_tree_input  and concepts to find
Output: ((concept1, verbal phrase, concept2), positions of the sentence)
"""
def find_matched_verbal_phrase(parse_tree_input, concepts_to_find, labels_map, debug, parse_tree_id=None):
    import codecs, re
    from get_parse_tree import preprocessText
    triplets = []
    pos1, pos2, parse_tree = parse_tree_input
    # making sure that in a specified parse tree each sentence will be processed separately
    # if we have 2 roots each for a sentence -> return 2 parse trees
    position_of_roots = [m.start() for m in re.finditer('\(ROOT ', parse_tree)]
    if len(position_of_roots) > 1:
        new_parse_trees = []
        i = 0
        while i < len(position_of_roots) - 1:
            if '(S' in parse_tree[position_of_roots[i]:position_of_roots[i+1]]:
                new_parse_trees.append(parse_tree[position_of_roots[i]:position_of_roots[i+1]])
            i += 1
        if '(S' in parse_tree[position_of_roots[i]:len(parse_tree)]:
            new_parse_trees.append(parse_tree[position_of_roots[i]:len(parse_tree)])
        if debug:
            print "Split of parse trees:", len(new_parse_trees)
    else:
        new_parse_trees = [parse_tree]

    # now, for each parse tree - usually only 1 parse tree
    for parse_tr in new_parse_trees:
        temp_literals = {}
        for term in concepts_to_find:
            norm_term = norm_literal(term)
            temp_literals[norm_term.encode('utf-8')] = 1
            for k, v in labels_map.items():
                if v[1] == norm_term:
                    temp_literals[k.encode('utf-8')] = 1

        tree_structure = parsetreenode.ParseTreeNode.parse(parse_tr)
        root = None
        for tree_structure in parsetreenode.ParseTreeNode.parse(parse_tr):
            if tree_structure.getText() != ".":
                root = tree_structure
        if not root:
            return triplets

        sentence = preprocessText(root.getText())

        # find occurrence of the concepts we are interested in (if any)
        tag_list = get_terms_from_string(sentence, temp_literals)
        tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]

        # replace synonyms by the main label in a normlized form - labels_map[...][1]
        tag_tuple_list_syn = []
        for el in tag_tuple_list:
            if el[0] in labels_map:
                tag_tuple_list_syn.append( (labels_map[el[0]][1], el[1], el[2]) )
            else:
                tag_tuple_list_syn.append((el[0], el[1], el[2]))

        if debug:
            print "--------------"
            print sentence
            print "Looking for: ", temp_literals
            print tag_tuple_list
            print tag_tuple_list_syn
        if len(set([l[0] for l in tag_tuple_list_syn])) < 2:
            return triplets

        # pair = [(('concept1', pos1, pos2, relative_position)),
        #   (('concept2', pos1, pos2, relative_position))]
        # where relative position is (?): ...climate(1) ...climate(2)...
        # For each pair of possible matches do the follwing 
        # find each concept/text node position and then identify if there NPVP between those nodes.
        for pair in get_pairs_from_concepts(tag_tuple_list_syn, concepts_to_find):
            if debug:
                print ">>>>>>>>>>>> Pair detected >>>>>>>>>>>"
                print parse_tr
                print pair
                print sentence[pair[0][1]:pair[0][2]], ";", sentence[pair[1][1]:pair[1][2]]
                print root.getText()
                print "<<<<<<<<<<<< Pair detected <<<<<<<<<<<"
            triplet = None
            try:
                index1 = pair[0][3]
                occur1 = root.findNodesForConcept(sentence[pair[0][1]:pair[0][2]])
                while index1 + 1 > len(occur1):
                    index1 = index1 - 1
                node1 = occur1[index1]
                if debug:
                    print node1.getText()
                index2 = pair[1][3]
                occur2 = root.findNodesForConcept(sentence[pair[1][1]:pair[1][2]])
                while index2 + 1 > len(occur2):
                    index2 = index2 - 1
                node2 = occur2[index2]
                if debug:
                    print node2.getText()
                triplet = findNpVpNpPatternFor(node1, node2, debug)
            except Exception, e:
                print e
            if triplet is not None:
                triplets.append(((triplet[0], triplet[1], triplet[2], pair[2]), pos1, pos2, parse_tree_id))
    return triplets

# TODO: add a remote file open function to return its text
def read_file_remotely_or_localy(file_path):
    import codecs
    return codecs.open(file_path, 'r', 'utf-8').read()

# simlar to find_matched_verbal_phrase
def find_matched_objects(parse_tree_input, subject, labels_map, statistics_previous, statistics_cleaned_previous, document_id, debug):
    import codecs, re
    from get_parse_tree import preprocessText
    objects_overall = []
    pos1, pos2, parse_tree = parse_tree_input
    position_of_roots = [m.start() for m in re.finditer('\(ROOT ', parse_tree)]
    if len(position_of_roots) > 1:
        new_parse_trees = []
        i = 0
        while i < len(position_of_roots) - 1:
            if '(S' in parse_tree[position_of_roots[i]:position_of_roots[i+1]]:
                new_parse_trees.append(parse_tree[position_of_roots[i]:position_of_roots[i+1]])
            i += 1
        if '(S' in parse_tree[position_of_roots[i]:len(parse_tree)]:
            new_parse_trees.append(parse_tree[position_of_roots[i]:len(parse_tree)])
        if debug:
            print "Split of parse trees:", len(new_parse_trees)
    else:
        new_parse_trees = [parse_tree]

    for parse_tr in new_parse_trees:
        temp_literals = {}
        norm_term = norm_literal(subject)
        temp_literals[norm_term.encode('utf-8')] = 1
        for k, v in labels_map.items():
            if v[1] == norm_term:
                temp_literals[k.encode('utf-8')] = 1

        tree_structure = parsetreenode.ParseTreeNode.parse(parse_tr)
        root = None
        for tree_structure in parsetreenode.ParseTreeNode.parse(parse_tr):
            if tree_structure.getText() != ".":
                root = tree_structure
        if not root:
            return objects_overall

        sentence = preprocessText(root.getText())

        tag_list = get_terms_from_string(sentence, temp_literals)
        tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]

        tag_tuple_list_syn = []
        for el in tag_tuple_list:
            if el[0] in labels_map:
                tag_tuple_list_syn.append( (labels_map[el[0]][1], el[1], el[2]) )
            else:
                tag_tuple_list_syn.append((el[0], el[1], el[2]))

        if debug:
            print "[[[[[[[[[[[[[[[[["
            print sentence
            print "Looking for: ", temp_literals
            print tag_tuple_list
            print tag_tuple_list_syn
        if len(set([l[0] for l in tag_tuple_list_syn])) == 0:
            return objects_overall

        # pair = [(('concept1', pos1, pos2, relative_position)),
        #   (('concept2', pos1, pos2, relative_position))]
        # where relative position is (?): ...climate(1) ...climate(2)...
        for subj in tag_tuple_list_syn:
            objs = {'front':[], 'back':[], 'triplet':None}
            if debug:
                print ">>>>>>>>>>>> Concept detected >>>>>>>>>>>"
                print parse_tr
                print sentence[subj[1]:subj[2]]
                print root.getText()
                print "]]]]]]]]]]]]]]]]]]"
            occur = root.findNodesForConcept(sentence[subj[1]:subj[2]])[0]
            npvpnp = occur.findNpVpNp()
            if npvpnp is not None:
                cleaned_triplet = clean_triplets([npvpnp])
                if cleaned_triplet:
                    # if cleaned_triplet[0].final_verbal_phrase in statistics_cleaned_previous.keys():
                    if npvpnp[0] != occur:
                        if len(npvpnp[0].getText().split(' ')) < 6 and CleanTriplet.is_alphanumeric(npvpnp[0].getText()) and npvpnp[0].tag != 'PRP':
                            objs['front'].append(npvpnp[0].getText().lower())
                            objs['triplet'] = (npvpnp[0], npvpnp[1], npvpnp[2], pos1, pos2, document_id)
                    else:
                        if len(npvpnp[2].getText().split(' ')) < 6 and CleanTriplet.is_alphanumeric(npvpnp[2].getText()) and npvpnp[2].tag != 'PRP':
                            objs['back'].append(npvpnp[2].getText().lower())
                            objs['triplet'] = (npvpnp[0], npvpnp[1], npvpnp[2], pos1, pos2, document_id)
            if objs['triplet'] is not None:
                objects_overall.append(objs)
    return objects_overall

class CleanTriplet:
    def __init__(self, triplet):
        import copy
        self.triplet = copy.deepcopy(triplet)
        self.prev_triplet = triplet
        self.words_between = self.triplet[1].getText().split(self.triplet[2].getText())[0].rstrip()
        self.verbal = self.triplet[1].getTextOfNotTagOnly('N')
        self.verb = []
        self.final_verbal_phrase = ""
        self.preposition = ""
        self.subject = self.triplet[3] if len(self.triplet) > 3 else -1
        self.negation = False

    # Identifies all nodes down the hierarchy with preposition tag
    # Extracts text of those nodes and stored to the seof.preposition
    # additionally function updates the final verbal phrases between concepts
    def identify_preposition(self):
        preps = self.triplet[1]._findDownReccursive(tag="PP", until=self.triplet[2])
        self.preposition = ';'.join([prep.getTextOfNotTagOnly(tag='N') for prep in preps])
        self.final_verbal_phrase = self.final_verbal_phrase.rstrip()

    @staticmethod
    def is_alphanumeric(final_verbal_phrase):
        import re
        valid = re.match('^[\w-]+$', final_verbal_phrase.replace(' ', '')) is not None
        return valid

    # Seeks for the verbal phrase in the Predicate node of the triplet
    # verbs in the predicate phrase are normalized and name is updated in the triplet object
    # output: updated triplet with normalized verbs
    def normalize_triplet(self):
        from nltk.stem.wordnet import WordNetLemmatizer
        lemmatizer = WordNetLemmatizer()
        verbs = self.triplet[1]._findDownReccursive(tag="VB", until=self.triplet[2])
        for verb in verbs:
            verb.text=lemmatizer.lemmatize(verb.text, 'v')
            verb.set_cached_text_for_parents(None)
        return self.triplet


    # should be called after the verbs are normalized
    def remove_aux_verbs_from_triplet(self):
        verbs = self.triplet[1]._findDownReccursive(tag="VB", until=self.triplet[2])
        for verb in verbs:
            if (verb.text.lower() == u'be' or verb.text.lower() == u'do' or verb.text.lower() == u'have'):
                parent = verb.delete_node() 
                parent.set_cached_text_for_parents(None)
        self.verb = [verb for verb in self.triplet[1]._findDownReccursive(tag="VB", until=self.triplet[2]) if verb.text]
        if self.triplet[1].getTextOfNotTagOnly('N'):
            self.final_verbal_phrase = self.triplet[1].getTextOfNotTagOnly('N') + self.preposition
        return self.triplet

    # simplistic version of negation, e.g., 'no', 'not' etc
    def is_negation(self):
        if self.final_verbal_phrase:
            if 'not' in self.final_verbal_phrase.split(' ') or 'no' in self.final_verbal_phrase.split(' ') or "n't" in self.final_verbal_phrase:
                self.negation = True

    def remove_modal_verbs(self):
        modal_verbs = self.triplet[1]._findDownReccursive(tag="MD", until=self.triplet[2])
        for current_node in modal_verbs:
            parent = current_node.delete_node()
            parent.set_cached_text_for_parents(None)
        return self.triplet

    @staticmethod
    def normalize_verbs(verbs):
        from nltk.stem.wordnet import WordNetLemmatizer
        lemmatizer = WordNetLemmatizer()
        return [lemmatizer.lemmatize(verb, 'v') for verb in verbs]

    # function could be used if we do not use the parse tree
    # therefore nltk POS is simply used to detect if the verbs are modal, or words are prepositions etc
    @staticmethod
    def clean_words_in_between(words_between):
        import nltk
        cleaned_words_between = []
        preposition = []
        for verb in CleanTriplet.normalize_verbs(words_between.split(' ')):
            if (verb.lower() != u'be' and verb.lower() != u'do' and verb.lower() != u'have'):
                if nltk.pos_tag([verb])[0][1] != 'MD':
                    cleaned_words_between.append(verb)
                if nltk.pos_tag([verb])[0][1] == 'IN':
                    preposition.append(verb)
        return ' '.join(cleaned_words_between), ';'.join(preposition)

""" 
Clean, normalize the triplets that are obtained for a concepts pair.
Function that is required for a cleaning
  - remove modal verbs
  - normalize verbs in triplet
  - remove have, be, do from the verbal phrase
"""
def clean_triplets(triplets):
    cleaned_triplets = []
    for triplet in triplets:
        cleaned_triplet = CleanTriplet(triplet)
        cleaned_triplet.remove_modal_verbs()
        cleaned_triplet.normalize_triplet()
        cleaned_triplet.remove_aux_verbs_from_triplet()
        cleaned_triplet.identify_preposition()
        if cleaned_triplet.final_verbal_phrase == "" and len(cleaned_triplet.words_between.split(' ')) < 5:
            cleaned_triplet.final_verbal_phrase, cleaned_triplet.preposition = CleanTriplet.clean_words_in_between(cleaned_triplet.words_between)
        cleaned_triplet.is_negation()
        if CleanTriplet.is_alphanumeric(cleaned_triplet.final_verbal_phrase):
            cleaned_triplets.append(cleaned_triplet)
    return cleaned_triplets










