import multiprocessing
import logging
import datetime
import os
import sys
import re
from functools import partial
import glob

LOG_FILE = "logs/parallel.log"
NUM_PROCS = int(sys.argv[2])
NUM_FILES = 124

"""
Clean a text file and return the list of words.
"""
def clean(inp):
	inp = inp.lower()
	inp = re.sub(r'[^a-z ]','',inp)
	return inp.split()

"""
Output the word count dictionary and set of words for a single file.
"""
def mapFun(filename):
    mycounts = dict()
    text = open(filename, 'r').read()
    text = clean(text)
    words = set()
    for word in text:
        if word in mycounts:
            mycounts[word] = mycounts[word]+1
        else:
            mycounts[word] = 1
        words.add(word)
    return mycounts, words

"""
Aggregate the counts for a single word across multiple files. Return (word, count).
"""
def reduceFun(mapOuts, word):
    count = 0
    for out in mapOuts:
        if word in out[1]:
            count = count + out[0][word]
    return word,count

def main():
    logging.basicConfig(filename=LOG_FILE, encoding="utf-8", level=logging.DEBUG)
    logging.info(f"main starting {datetime.datetime.now()}")
    pool = multiprocessing.Pool(processes=NUM_PROCS)
    dataDir = sys.argv[1]
    files = glob.glob(glob.escape(dataDir) + "/*")
    mapOuts = pool.map(mapFun, files)
    words = set()
    for out in mapOuts:
        words = words.union(out[1])
    redOuts = pool.map(partial(reduceFun, mapOuts), words)
    sortedWords = sorted(redOuts, key=lambda x: x[1], reverse=True)
    for w in sortedWords[:10]:
        print(w)
    logging.info("main finished")

if __name__=="__main__":
    main()