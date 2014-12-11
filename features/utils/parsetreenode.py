class ParseTreeNode:
    '''
    (ROOT (INTJ (UH .)))   (ROOT (S (ADVP (RB However)) (, ,) (NP (NP (JJ ephemeral) (NNS habitats)) (NP (NN heuristic)
    (NN framework))) (VP (VBZ remains) (ADJP (RB largely) (JJ untested))) (. .)))
    advmod(untested-9, However-1) amod(habitats-4, ephemeral-3) nsubj(untested-9, habitats-4)
    nn(framework-6, heuristic-5) dep(habitats-4, framework-6) cop(untested-9, remains-7)
    advmod(untested-9, largely-8) root(ROOT-0, untested-9)
    '''
    def __init__(self, tag):
        self.tag = tag
        self.text = ""
        self.cachedText = None
        self.children = []
        self.parent = None

    def getText(self):
        if self.cachedText is None:
            textList = []
            self._text_visitor(textList)
            self.cachedText = " ".join(textList)
        return self.cachedText

    def _text_visitor(self, list):
        if self.text != "":
            list.append(self.text)
        for child in self.children:
            child._text_visitor(list)

    def getTextOfTagOnly(self, tag):
        textList = []
        self._textTag_visitor(tag, textList)
        return " ".join(textList)

    def _textTag_visitor(self, tag, list):
        if not self.tag.startswith(tag):
            return
        if self.text != "":
            list.append(self.text)
        for child in self.children:
            child._textTag_visitor(tag, list)

    def findNodesForConcept(self, concept):
        resultList = []
        self._findNodesForConceptVisitor(concept, resultList)
        return resultList

    # changed only for this application to find occurrences of a concept lowered
    def _findNodesForConceptVisitor(self, concept, resultList):
        if concept in self.getText():
            found = False
            for child in self.children:
                if concept in child.getText():
                    found = True
                    child._findNodesForConceptVisitor(concept, resultList)
            if not found:
                resultList.append(self)

    def findMyParentSiblingFor(self, node):
        if self.parent is None:
            return None
        visitor = self
        while visitor.parent is not None:
            if node in visitor.parent.children:
                return visitor
            visitor = visitor.parent
        return None

    def commonParent(self, node2):
        visitedNodes = {}
        visitor = self
        while visitor is not None:
            visitedNodes[visitor] = True
            visitor = visitor.parent
        visitor = node2
        while visitor is not None:
            if visitedNodes.has_key(visitor):
                return visitor
            visitor = visitor.parent
        return None

    def _findUp(self, tag='S'):
        node = self
        while node is not None:
            if node.tag == tag:
                return node
            else:
                node = node.parent
        return None

    def _findDown(self, tag='NP', ignore=[]):
        node = self
        for i in ignore:
            if node.tag == i:
                return None
        if node.tag.startswith(tag):
            return node
        else:
            for child in node.children:
                result = child._findDown(tag, ignore)
                if result is not None:
                    return result
        return None

    def _findDownReccursive(self, tag='NP', ignore=[], until=None):
        queue = [self]
        tags = []
        while queue:
            current = queue.pop(0)
            if current == until:
                break
            if current.tag.startswith(tag):
                tags.append(current)
            queue.extend(current.children)
        return tags

    def delete_node(self):
        self.parent.children.remove(self)
        self.parent.children.extend(self.children)
        return self.parent

    def findNpVpNp(self):
        s = self._findUp(tag='S')
        if s is None:
            return None

        np1 = None
        vp = None
        for child in s.children:
            if child.tag == 'NP':
                np1 = child
            elif child.tag == 'VP':
                vp = child
        if np1 is None or vp is None:
            return None

        np2 = vp._findDown(tag='NP', ignore=['S'])
        if np2 is None:
            return None

        return (np1, vp, np2)

    def getTextOfNotTagOnly(self, tag):
        textList = []
        self._textNotTag_visitor(tag, textList)
        return " ".join(textList).split(',')[0]

    def _textNotTag_visitor(self, tag, list):
        if self.tag.startswith(tag):
            return
        if self.text != "":
            list.append(self.text)
        for child in self.children:
            child._textNotTag_visitor(tag, list)

    def set_cached_text_for_parents(self, cached_text):
        self.cachedText = cached_text
        current = self
        while current.parent is not None:
            current.parent.cachedText = cached_text
            current = current.parent

    @staticmethod
    def parse(line):
        listRoots = []
        left = 0
        while left < len(line):
            if line[left].isspace():
                left += 1
                continue
            if line[left] == "(":
                node, left = ParseTreeNode._inner_parse_node(line, left + 1)
                listRoots.append(node)
            else:
                break
        return listRoots

    @staticmethod
    def _inner_parse_node(line, pos):
        left = pos
        tag = ""
        while not line[left].isspace():
            tag += line[left]
            left += 1
        node = ParseTreeNode(tag)
        while line[left] != ")":
            while line[left].isspace():
                left += 1
            if line[left] == "(":
                childNode, left = ParseTreeNode._inner_parse_node(line, left + 1)
                childNode.parent = node
                node.children.append(childNode)
            else:
                text = ""
                while line[left] != ")":
                    text += line[left]
                    left += 1
                node.text = text
        # TODO: Add parsing of the dependency tree
        return node, left + 1