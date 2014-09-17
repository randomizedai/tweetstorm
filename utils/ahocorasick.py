'''
Created on Sep 17, 2014

@author: Amit
'''


from collections import deque,defaultdict
import cPickle

def uniquelist_of_dicts (list_dicts,keyfunction,scorefunction):
    unique_list_dicts = []
    unique_list_keys = []
    unique_list_maxscore = []
    for temp_dict in list_dicts:
        temp_key = keyfunction(temp_dict)
        if temp_key not in unique_list_keys:
            unique_list_dicts.append(temp_dict)
            unique_list_keys.append(temp_key)
            unique_list_maxscore.append(scorefunction(temp_dict))
        else:
            index_key = unique_list_keys.index(temp_key)
            prev_score = unique_list_maxscore[index_key]
            cur_score = scorefunction(temp_dict)
            if cur_score > prev_score:
                unique_list_dicts[index_key] = temp_dict
                unique_list_maxscore[index_key] = cur_score    
    return unique_list_dicts

class State:


    def __init__(self ,sid, val):
        self.sid = sid
        self.value = val
        self.tranList = []
        self.failState = 0
        self.outputSet = set()
        self.liberalOutputSet = set()
        self.kw_tranList_map = {}

    def getTransition(self, val,ngramindex,id_to_state,strict):
        """ this function gets the next state on input val"""
        vals_list = []
        if isinstance(val,list):
            vals_list = val
        else:
            vals_list = [val]
        
        if len(self.tranList) == 0:
            return None
       
        for testval in vals_list:
            for testval in vals_list:
                if testval in self.kw_tranList_map:
                    return self.tranList[self.kw_tranList_map[testval]]
        
        return None
  
    def getTransitionAndAdd(self, val,ngramindex,id_to_state,strict):
        """ this function gets the next state on input val, also adds"""
        if isinstance(val,list):
            vals_list = val
        else:
            vals_list = [val]
        
        for testval in vals_list:
            if testval in self.kw_tranList_map:
                return self.tranList[self.kw_tranList_map[testval]]
        
        return None  

    def testTransition(self, val,ngramindex,id_to_state,strict):
        """ This checks whether there is transition or not on input val"""
        """ for current state, the transition is always true on any input"""
        vals_list = []
        if isinstance(val,list):
            vals_list = val
        else:
            vals_list = [val]
            
        if self.sid == 0:     
            return True
        else:
            if len(self.tranList) == 0:
                return False
        
            for testval in vals_list:
                if testval in self.kw_tranList_map:
                    return True
                 
            return False
    
    def addToTranList(self, new_state):
        self.tranList.append(new_state)
        self.kw_tranList_map[new_state.value[0]] = len(self.tranList) - 1    
        
    def addOutput(self, key):
        """This adds the key to the output in the state"""
        self.outputSet = self.outputSet ^ key

    def addLiberalOutput(self, key):
        """This adds the key to the output in the state"""
        self.liberalOutputSet = self.liberalOutputSet ^ key
                
    def setLiberalMatches(self):
        for child in self.tranList:
            child.setLiberalMatches()
        if len(self.tranList) == 1 and not self.outputSet:
            output_set = set([(y[0],y[1] - 1) for y in self.tranList[0].outputSet if y[1] > 1])
            self.addLiberalOutput(output_set)
        
    def get_outputs(self,j,kw_to_reference):  
        matched_keys = []
        if self.liberalOutputSet != None and (len(self.liberalOutputSet) != 0):
            itr = iter(self.liberalOutputSet)
            for output in itr:
                token_endpos = j 
                token_beginpos = j - output[1] + 1
                key = output[0]
                for a,b in kw_to_reference[key]:
                    matched_keys.append({'beginpos':token_beginpos,'endpos':token_endpos+1,'key':key,'match':(a,b), 'score':1})
               #     print "LIBERAL OUTPUT -->", matched_keys[-1]
          
        #print "ALSD<A",self.outputSet            
        if self.outputSet != None and (len(self.outputSet) != 0):
            itr = iter(self.outputSet)
            for output in itr:
                token_endpos = j 
                token_beginpos = j - output[1] + 1
                key = output[0]
                #print key
                for a,b in kw_to_reference[key]:
                    matched_keys.append({'beginpos':token_beginpos,'endpos':token_endpos+1,'key':key,'match':(a,b),'score':1})
                    #print "OUTPUT -->", matched_keys[-1]
            
        return matched_keys
     
class ahoCorasick(object):
    #root = None
    #newstate = None
    #kw_to_reference = defaultdict(list)
    #
    #id_to_state = {}
    #strict = True
    #stem = False
    
    def __init__(self):
        self.root = State(0, [' '])
        self.newstate = 0
        self.id_to_state = {}
        self.id_to_state[self.newstate] = self.root
      #  self.ngramindex = NgramIndex()
        self.kw_to_reference = defaultdict(list)
        self.ngramindex = None
        self.strict = True
        self.stem = False
    
        
    def addKeyword(self, keyword):
        """Adds the keyword in the tree"""
        
        j = 0
       # state = 0
        current = self.root
        clean_key = keyword
        tokens = clean_key.split()
        if self.stem:
            tokens = [stem(token) for token in tokens]
        
        while j < len(tokens):
            cur_token = tokens[j]
            j = j + 1
            child = current.getTransitionAndAdd(cur_token,self.ngramindex,self.id_to_state,self.strict)
            if child != None:
                current = child
            else:
                self.newstate = self.newstate + 1
                nd = State(self.newstate, [cur_token])
                self.id_to_state[self.newstate] = nd
             #   self.ngramindex.addKeyword(cur_token, nd.sid, "")
                current.addToTranList(nd)
                
                current = nd
                while j < len(tokens):
                    self.newstate = self.newstate +1
                    nd2 = State(self.newstate, [tokens[j]])
                    self.id_to_state[self.newstate] = nd2
              #      self.ngramindex.addKeyword(tokens[j], nd2.sid, "")
                    current.addToTranList(nd2)
                    current = nd2
                    j = j+1
                break
        current.outputSet.add((keyword,len(tokens)))
        
        
        
        
        
        
    def setFailTransitions(self,debug=False,strict=False):
        """Sets the fail transitions in tree"""
        if not strict:
            self.root.setLiberalMatches()
        queue = deque()
        current = self.root
        child = self.root

        for nd in self.root.tranList:
            queue.append(nd)
            nd.failState = self.root

        while len(queue) != 0:
            r = queue.popleft()
            for nd in r.tranList:
                queue.append(nd)
                state = r.failState
                val = nd.value
                ##print "setting fail transition for -->", val
                current = state
                while True:
                 #   #print "IN LOOP",current.value,current.sid
                    if current.testTransition(val,self.ngramindex,self.id_to_state,self.strict) == False:
                        current = current.failState
                    else:
                        break
                child = current.getTransition(val,self.ngramindex,self.id_to_state,self.strict)
                if child == None:
                    nd.failState = current
                    #if (nd.sid == current.sid):
                       # pdb.set_trace()
 
                else:
                    nd.failState = child
                    #if (nd.sid == child.sid):
                      #  pdb.set_trace()
                nd.addOutput(nd.failState.outputSet)
                nd.addOutput(nd.failState.liberalOutputSet)
       

    def tag(self, string,debug=False):
        """ Finds all substrings of input which are keywords in the tree"""
        
        current = self.root
        j = 0
        matched_keys = []
      #  clean_text = get_clean_string(string)
        tokens = string.split()
        if self.stem:
            tokens = [stem(token) for token in tokens]
        while j < len(tokens):
            if debug:
                print "token No. --> ", j, " --> ", tokens[j]
            while True:
                #print "current State",current,
                #print current.sid
                if current.testTransition(tokens[j],self.ngramindex,self.id_to_state,self.strict) == False:
                    if current != None:
                        matched_keys += current.get_outputs(j-1,self.kw_to_reference)
                     #   #print "AAA",matched_keys     
                    current = current.failState
                else:
                    child = current.getTransition(tokens[j],self.ngramindex,self.id_to_state,self.strict)
                    
                    break
            if child != None:
                #print "going to ",child.sid,child.value, child.outputSet  
    
                if debug:
                    print "Transition to val ==", child.value,child.sid
                current = child
                matched_keys += current.get_outputs(j,self.kw_to_reference)
                #print current
                #print "BBB",matched_keys
                
            j = j + 1
        return uniquelist_of_dicts(matched_keys, lambda x : x['key'] + "__" + str(x['match'][1]) + "__" + str(x['beginpos']), lambda x : x['endpos'] - x['beginpos'])
        
        
    def displayTree(self):
        """ It is used to display the tree of keywords. Prints ID of node and value of node"""
        queue = deque()
        for nd in self.root.tranList:
            queue.append(nd)

        while len(queue) !=0:
            node = queue.popleft()
            for nd in node.tranList:
                queue.append(nd)
            print node.sid, node.value
            
                
    def displayOutput(self):
        """ This function displays the outputs at a state"""
        queue = deque()
        for nd in self.root.tranList:
            queue.append(nd)

        while len(queue) !=0:
            node = queue.popleft()
            for nd in node.tranList:
                queue.append(nd)
            
            itr = iter(node.outputSet)
            if len(node.outputSet) !=0:
                print node.sid
            for string in itr:
                print string



         
def getTagger(list_keywords,debug=False):
    tagger = ahoCorasick()
    for i,kw in enumerate(list_keywords):
        tagger.addKeyword(kw)
        tagger.kw_to_reference[kw].append((i,kw))
    
    tagger.setFailTransitions(debug, strict=True)    
        
    return tagger 

    
     
if (__name__ == "__main__"):
    list_keywords = ["can't","climate change", "change of climate", "kyoto protocol", "ipcc", "el nino"]
    tagger = getTagger(list_keywords)
    l = tagger.tag("kyoto protocol and climate change of climate in ipcc ipcc and ipcc")
    print l
    
