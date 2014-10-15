'''
Created on Oct 2, 2014

@author: gupta
'''
import sys

if __name__ == '__main__':
    fn = sys.argv[1]
    dictv = {}
    with open(fn) as fp:
        for line in fp:
            tokens = line.split("\"")
            if tokens[1] not in dictv:
                dictv[tokens[1]] = 1
                print line.strip()