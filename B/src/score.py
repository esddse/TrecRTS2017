
import sys
import os
import time
import datetime
import time
import json
import MySQLdb
sys.path.append('./')
sys.path.append('../')
#sys.path.append('./src/')
from package.query import Query
from package.advancedTweet import AdvancedTweet
from package.utils import load_stopword_set, load_corpus_dict, load_stem_idf
from package.relation import dir_score, jm_score, cosine_score, overlap_score

# -----------------------config -----------------------
# -----------------------------------------------------

task_start_month = 7
task_start_day   = 29
task_end_month   = 8
task_end_day     = 5
day_span         = 8

def date2day(month, day, config=''):
    if config == 'test':
        return day - 1 
    else:
        if month == task_start_month:
            return day - task_start_day + 1
        else:
            return day + 3


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

# ----------------------- score -----------------------
# -----------------------------------------------------

def sim_q_t(query, tweet):
    score_dir     = dir_score(query, tweet, corpus_dict)
    score_jm      = jm_score(query, tweet, corpus_dict)
    score_cosine  = cosine_score(query, tweet, idf_dict)
    score_overlap = overlap_score(query, tweet)
    return score_dir, score_jm, score_cosine, score_overlap

# ------------------------------------------------------

def run(query, tweet, f):
    score_dir, score_jm, score_cosine, score_overlap = sim_q_t(query, tweet)
    string = "\t".join([
            tweet.created_at,
            tweet.id_str,
            " ".join(tweet.word_list),
            " ".join(tweet.stem_list),
            str(score_dir),
            str(score_jm),
            str(score_cosine),
            str(score_overlap),
            str(len(query.expanded_tf))
        ])
    f.write(string+'\n')

# ------------------------------------------------------
def is_overlap(query, tweet):
    for stem in query.stem_distri:
        if stem in tweet.stem_distri:
            return True
    return False

def tweet_handle(curday, tweet):
    for query in query_list:
        filepath = './RUN/score/%d/%s' % (curday, query.id)
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f: pass
        if is_overlap(query, tweet):
            with open(filepath, 'a') as f:
                run(query, tweet, f)

def is_quick_filtered(tweet):
    for stem in tweet.stem_distri:
        if stem in all_stem_set:
            return False
    return True

def row_handle(created_at, id_str, word_list_str, stem_list_str):
    dt = datetime.datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
    curday = date2day(dt.month, dt.day)
    if curday >= 1 and curday <= day_span:
        tweet = AdvancedTweet(created_at, id_str, word_list_str, stem_list_str)
        if not is_quick_filtered(tweet):
           tweet_handle(curday, tweet)

def main():
    try:
        conn = MySQLdb.connect(host='localhost', user='', passwd='', db='trec17', port=3306)
        cur  = conn.cursor()

        cnt = 0
        rows = None
        while rows != '':
            cur.execute('SELECT * FROM preprocess WHERE is_process=1 LIMIT 100000')
            rows = cur.fetchall()

            print "%d tweets fetched" % (len(rows))

            for row in rows:
                id            = row[0]
                created_at    = row[1]
                id_str        = row[2]
                word_list_str = row[3]
                stem_list_str = row[4]
                row_handle(created_at, id_str, word_list_str, stem_list_str)
                cur.execute('UPDATE preprocess SET is_process=2 WHERE id=%d' % id)

                cnt += 1
                if cnt % 100000 == 0:
                    print cnt, " tweeted processed"
            conn.commit()

        cur.close()
        conn.close()
        print 'process %d rows' % len(rows)
        return "continue"
    
    except Exception, e:
        print "error while loading tweets from mysql !"
        print e

if __name__ == '__main__':
    main()

    