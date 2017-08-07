import sys
import os
import time
import datetime
import time
import json
import MySQLdb
import threading
sys.path.append('./')
sys.path.append('../')
#sys.path.append('./src/')
from multiprocessing import Process
from package.query import Query
from package.candidate import Candidate
from package.advancedTweet import AdvancedTweet
from package.utils import load_stopword_set, load_corpus_dict, load_stem_idf
from package.relation import sym_dir_score, sym_jm_score, sym_cosine_score, sym_overlap_score


# --------------- config -----------------------
# ----------------------------------------------

rel_method = "mix"
rel        = 0.71
nol_method = "mix"
nol        = 0.72

mix_ratio = 0.5

runTag     = "PKUICSTRunB3"
input_dir  = "./RUN/score_rf/"
output_dir = "./RUN/rank/" + runTag + '/'

if not os.path.exists(output_dir):
	os.mkdir(output_dir)

# -------------------- global ----------------------------
# --------------------------------------------------------

stopword_set = load_stopword_set()
print 'stopword_set size: %d' % len(stopword_set)

corpus_dict = load_corpus_dict()
print 'corpus_dict size: %d'  % len(corpus_dict)

idf_dict = load_stem_idf()
print 'idf_dict size: %d' % len(idf_dict)

def load_query_list():
    query_list = []
    content = open('../data/topics_2017').read()
    query_json_list = json.loads(content)
    for query_json in query_json_list:
        query_json_str = json.dumps(query_json)
        query = Query(query_json_str, stopword_set)
        if query.is_valid:
            query_list.append(query)
    return query_list

query_list = load_query_list()
print 'query_list size: %d' % len(query_list)

def load_all_stem_set():
    all_stem_set = set()
    for query in query_list:
        for stem in query.stem_distri:
            all_stem_set.add(stem)
    return all_stem_set
all_stem_set = load_all_stem_set()
print 'all_stem_set size: %d' % len(all_stem_set)

def init_history():
	history = {}
	for query in query_list:
		history[str(query.id)] = {}
		for day in range(1, 9):
			if not os.path.exists(output_dir+str(day)+'/'):
				os.mkdir(output_dir+str(day)+'/')
			history[query.id][str(day)] = []
			with open(output_dir+str(day)+'/'+query.id, "w") as f: pass
	return history
history = init_history()

def save_history_log(qid, day, tweet, score):
	history[qid][day].append(tweet)
	filepath = output_dir + str(day) + "/" + qid
	with open(filepath, 'a') as f:
		created_at    = tweet.created_at
		id_str        = tweet.id_str
		word_list_str = ' '.join(tweet.word_list)
		stem_list_str = ' '.join(tweet.stem_list)
		f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (qid, day, created_at, id_str, word_list_str, stem_list_str, score))

# ---------------------- rank --------------------------
# ------------------------------------------------------

def sim_t_t(tweet_1, tweet_2):
	if nol_method == "jm":
		return sym_jm_score(tweet_1, tweet_2, corpus_dict)
	elif nol_method == 'dir' or nol_method == 'mix':
		return sym_dir_score(tweet_1, tweet_2, corpus_dict)
	elif nol_method == 'cos':
		return sym_cosine_score(tweet_1, tweet_2, idf_dict)
	elif nol_method == 'overlap':
		return sym_overlap_score(tweet_1, tweet_2)

def novel(curtweet, recommend_history):
	max_score = 0
	for tweet in recommend_history:
		score = sim_t_t(curtweet, tweet)
		max_score = max(max_score, score)
	if max_score < nol: 
		return True
	else: 
		return False

def rank(topic_path, topic_id, day):

	candidate_list = []
	with open(topic_path, "r") as f:
		for line in f:
			created_at, id_str, word_list_str, stem_list_str, dirichlet, jm, cosine, overlap, len_query_term = line.strip().split('\t')

			if float(dirichlet) < rel and rel_method == 'dir': continue
			if float(jm) < rel and rel_method == 'jm': continue
			if float(cosine) < rel and rel_method == 'cos': continue
			if float(overlap) < min(rel, float(len_query_term)) and rel_method == 'overlap': continue
			if float(cosine) * mix_ratio + float(dirichlet) * (1 - mix_ratio) < rel and rel_method == 'mix': continue

			tweet = AdvancedTweet(created_at, id_str, word_list_str, stem_list_str)
			candidate = Candidate(tweet, float(dirichlet), float(jm), float(cosine), float(overlap))
			candidate_list.append(candidate)
	if rel_method == 'jm':
		candidate_list.sort(key=lambda candidate: candidate.jm, reverse=True)
	elif rel_method == 'dir':
		candidate_list.sort(key=lambda candidate: candidate.dirichlet, reverse=True)
	elif rel_method == 'cos':
		candidate_list.sort(key=lambda candidate: candidate.cosine, reverse=True)
	elif rel_method == 'overlap':
		candidate_list.sort(key=lambda candidate: candidate.overlap, reverse=True)
	elif rel_method == 'mix':
		candidate_list.sort(key=lambda candidate: candidate.cosine*mix_ratio + candidate.dirichlet*(1-mix_ratio), reverse=True)


	recommend_history = []
	for key in history[topic_id]:
		if int(key) < int(day) and int(key) > 1:
			recommend_history += history[topic_id][key]

	selected_num = 0
	for candidate in candidate_list:
		if selected_num >= 100: break 
		tweet = candidate.tweet
		if novel(tweet, recommend_history):
			score = None
			if   rel_method == 'jm':      score = candidate.jm
			elif rel_method == 'dir':     score = candidate.dirichlet
			elif rel_method == 'cos':     score = candidate.cosine 
			elif rel_method == 'overlap': score = candidate.overlap
			elif rel_method == 'mix':     score = candidate.cosine*mix_ratio + candidate.dirichlet*(1-mix_ratio)
			save_history_log(topic_id, day, tweet, score)
			recommend_history.append(tweet)
			selected_num += 1

def rank_day(day_dir, day):
	topic_ids = [topic_id for topic_id in os.listdir(day_dir)]
	for topic_id in topic_ids:
		topic_path = day_dir + topic_id
		rank(topic_path, topic_id, str(day))

# -------------------------------------------------------

def main():
	days = [int(day) for day in os.listdir(input_dir)]
	days.sort()

	processes = []
	for day in days:
		day_dir = input_dir + str(day) + "/"
		
		p = Process(target=rank_day, args=(day_dir, day))
		processes.append(p)
		p.start()
	for p in processes:
		p.join()
		


if __name__ == "__main__":
	main()