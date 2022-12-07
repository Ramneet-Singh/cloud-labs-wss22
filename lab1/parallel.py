import multiprocessing
import logging
import datetime
import sys
import re
import glob

LOG_FILE = "logs/parallel.log"
NUM_PROCS = int(sys.argv[2])
NUM_FILES = 124

"""
Clean a text file and return the list of words.
"""
def clean(inp):
    inp = inp.lower()
    matches = re.findall(r'(\b[^\s]+\b)', inp)
    return matches

"""
Output the word count dictionary for a single file.
"""
def mapFun(filename):
    mycounts = dict()
    text = open(filename, 'r').read()
    text = clean(text)
    for word in text:
        if word in mycounts:
            mycounts[word] = mycounts[word]+1
        else:
            mycounts[word] = 1
    return mycounts

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
    counts = dict()
    for out in mapOuts:
        for w in out:
            if w in counts:
                counts[w] = counts[w]+out[w]
            else:
                counts[w] = out[w]
    sortedWords = sorted(counts.items(), key=lambda x : x[1], reverse=True)
    for w in sortedWords[:10]:
        print(w)
    logging.info("main finished")

if __name__=="__main__":
    main()