import subprocess
import sys, os
import codecs
import time
from tempfile import NamedTemporaryFile

PATH=''


def detect_sentences(text):
    dot_list = []
    for i, c in enumerate(text):
        if c == "." or c == "!" or c == "?":
            if text[i + 1:i + 2] == ' ' or text[i + 1:i + 2] == '\n' or text[i + 1:i + 2] == '\"':
                dot_list.append(i)
        if c == '\n' and not (text[i + 1:i + 2] == ' ' or text[i + 1:i + 2] == '\n' or text[i + 1:i + 2] == '\"'):
            dot_list.append(i)
    return dot_list

def detect_paragraph(text):
    import re
    paragraph_list = []
    for match in re.finditer('\n\s{3,}', text):
        paragraph_list.append((match.start(), match.end()))
    return paragraph_list    

def preprocessText(text):
    # Clean the sentence from the stopwords
    sw = []
    content = [w for w in text.split(" ") if w.lower() not in sw]
    content_without_brackets = []
    i = 0
    while i < len(content):
        el = content[i]
        if "(" not in el:
            content_without_brackets.append(el)
            i += 1
        else:
            while i < len(content) and ")" not in content[i]:
                i += 1
            i += 1

    return " ".join(content_without_brackets)

def main():
    if len(sys.argv) != 2:
        print "Usage: script <file with locations>"
        sys.exit(-1)
    for location in open(sys.argv[1], 'r'):
        print ""
        print "----------------------"
	print "Processing next file: " + location
        print "----------------------"
        print ""
        location = PATH + location
        parse_file(location.strip())
        
def parse_file(location, path_to_parser="/home/iuliia.proskurnia/stanford-parser-2012-11-12/lexparser.sh"):
    with codecs.open(location, 'r', 'utf-8') as fh:
        text = fh.read()
    dot_list = detect_sentences(text)
    tag_tuple_list = []
    for el in dot_list:
        tag_tuple_list.append(('.', el, el + 1))
    sorted_tag_list = sorted(tag_tuple_list, key=lambda x: x[1])
    num_elems = len(sorted_tag_list)
    
    separator = "_____@@@@@_____"
    with codecs.open(location + ".parse_tree", 'w', 'utf-8') as f_parse_tree_out:
        i = -1
        while i < num_elems:
            k = i + 1
            if k < num_elems:
                index = 0 if i < 0 else sorted_tag_list[i][2] + 1
                sen = text[index:sorted_tag_list[k][2]]
                preprocessed = preprocessText(sen)
                f = NamedTemporaryFile(delete=False)
                filename = f.name
                f.close()
                with codecs.open(filename, 'w', 'utf-8') as fh:
                    fh.write(preprocessed)
                    fh.flush()
                    fh.seek(0)
                    parse_tree = subprocess.Popen([path_to_parser, fh.name], stdout=subprocess.PIPE).stdout.read().decode("utf-8").encode('ascii', 'ignore')
                os.unlink(filename)
                f_parse_tree_out.write('%s%s%d_%d%s%s%s%s\t' % (location, separator, index, sorted_tag_list[k][2], separator, sen, separator, parse_tree.replace('\n', ' ')))
            i += 1

if __name__ == "__main__":
    main()