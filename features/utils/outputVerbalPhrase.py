import sys
import os
# NOTE: We need to have get_parse_tree file in the code folder
sys.path.append(".")
sys.path.append("/opt/texpp")
from get_parse_tree import preprocessText
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
def parse_tree_from_file(path_to_parse_trees, separator):
    import os
    # open file to read the parse tree for the sentence
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
            print sibling1.getText()
        if sibling1.tag.startswith('V'):
            return node2, sibling1, node1
    else:
        sibling2 = pnode2.findMyParentSiblingFor(pnode1)
        if debug:
            print "Sibling found"
            print sibling2.getText()
        if sibling2 is not None and sibling2.tag.startswith('V'):
            return node1, sibling2, node2
    return None

# For given list of concepts in the sentence, the function returns
# the list of concepts pairs to be checked on the presentece of relation between
def get_pairs_from_concepts(tag_tuple_list):
    concepts_map = {}
    new_tag_tuple_map = {}
    concept_names = list(set([l[0] for l in tag_tuple_list]))
    if len(concept_names) == 2:
        for con in concept_names:
            concepts_map[con] = 0
            new_tag_tuple_map[con] = []
        for el in tag_tuple_list:
            new_tag_tuple_map[el[0]].append((el[0], 
                el[1], 
                el[2], 
                concepts_map[el[0]]))
            concepts_map[el[0]] = concepts_map[el[0]] + 1

        for positionA in new_tag_tuple_map[concept_names[0]]:
            for positionB in new_tag_tuple_map[concept_names[1]]:
                if positionA[1] < positionB[1]:
                    yield positionA, positionB
                else:
                    yield positionB, positionA
        

"""
Function to be run in parallel
Input: Given the text of the file, sentence positions and parse tree as parse_tree_input  and concepts to find
Output: ((concept1, verbal phrase, concept2), positions of the sentence)
"""
def find_matched_verbal_phrase(parse_tree_input, concepts_to_find, labels_map, debug):
    import codecs, re
    triplets = []
    pos1, pos2, parse_tree = parse_tree_input
    position_of_roots = [m.start() for m in re.finditer('\(ROOT ', parse_tree)]
    if len(position_of_roots) > 1:
        new_parse_trees = []
        for i in range(len(position_of_roots) - 1):
            new_parse_trees.append(parse_tree[position_of_roots[i]:position_of_roots[i+1]])
        new_parse_trees.append(parse_tree[position_of_roots[i]:len(parse_tree)])
        if debug:
            print "Split of parse trees:", len(new_parse_trees)
    else:
        new_parse_trees = [parse_tree]

    for parse_tr in new_parse_trees:
        temp_literals = {}
        for term in concepts_to_find:
            norm_term = norm_literal(term)
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

        tag_list = get_terms_from_string(sentence, temp_literals)
        tag_tuple_list = [(l.value, l.start, l.end) for l in tag_list]

        tag_tuple_list_syn = []
        for el in tag_tuple_list:
            tag_tuple_list_syn.append( (labels_map[el[0]][1], el[1], el[2]) )

        if debug:
            print "--------------"
            print sentence
            print "Looking for: ", temp_literals
            print tag_tuple_list
            print tag_tuple_list_syn
        if len(set([l[0] for l in tag_tuple_list_syn])) != 2:
            return triplets

        # pair = [(('concept1', pos1, pos2, relative_position)),
        #   (('concept2', pos1, pos2, relative_position))]
        # where relative position is (?): ...climate(1) ...climate(2)...
        for pair in get_pairs_from_concepts(tag_tuple_list_syn):
            if debug:
                print ">>>>>>>>>>>> Pair detected >>>>>>>>>>>"
                print parse_tr
                print pair
                print sentence[pair[0][1]:pair[0][2]], ";", sentence[pair[1][1]:pair[1][2]]
                print root.getText()
                print "-------------"
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
                while index2 + 1> len(occur2):
                    index2 = index2 - 1
                node2 = occur2[index2]
                if debug:
                    print node2.getText()
                triplet = findNpVpNpPatternFor(node1, node2, debug)
            except Exception, e:
                print e
            if triplet is not None:
                triplets.append((triplet, pos1, pos2))
    return triplets

# TODO: add a remote file open function to return its text
def read_file_remotely_or_localy(file_path):
    import codecs
    return codecs.open(file_path, 'r', 'utf-8').read()