import os
import sys
import json

def load_stopword_set():
    print("load stopwords ...")
    absolute_path = os.path.join(os.path.dirname(__file__) + "/../../data/stopword")
    stopword_set = set()
    with open(absolute_path, "r") as f:
        for line in f:
            stopword_set.add(line.strip())
    print("load stopwords over")
    return stopword_set

def load_corpus_dict():
    print("load corpus ...")
    corpus_dict = {}
    total_count = 0
    line_no = 0
    with open("../data/stem_tf", "r") as f:
        for line in f:
            line_no += 1
            if line_no == 1:
                total_count = float(line.strip())
            else:
                t = line.strip().split('\t')
                corpus_dict[t[0]] = float(t[1]) / total_count
    print("load corpus over")
    return corpus_dict

def load_stem_idf():
    print("load idf ...")
    stem_idf = {}
    with open("../data/stem_idf", "r") as f:
        f.readline()
        for line in f:
            line = line.strip().split('\t')
            stem_idf[line[0]] = float(line[1])
    print("load idf over")
    return stem_idf
