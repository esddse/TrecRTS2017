import os
import sys
import time
import json
import MySQLdb
import datetime
sys.path.append('./')
sys.path.append('../')
sys.path.append('./src/')
from package.query import Query
from package.advancedTweet import AdvancedTweet
from package.utils import load_stopword_set, load_corpus_dict
from package.relation import dir_score, sym_dir_score

######################################################### Config
client_id              = 'MmSNsCe2OoPn'
submit_log_file_path   = 'RUN/SUBMIT.log'

rel_thr = 0.7
red_thr = 0.5
mu      = 50

task_start_month = 7
task_start_day   = 29
task_end_month   = 8
task_end_day     = 5
day_span         = 8

def date2day(month, day):
    if month == task_start_month:
        return day - task_start_day + 1
    else:
        return day + 3

######################################################### Load Global Variates
stopword_set = load_stopword_set()
print 'stopword_set size: %d' % len(stopword_set)

corpus_dict = load_corpus_dict()
print 'corpus_dict size: %d'  % len(corpus_dict)

def load_query_list():
    query_list = []
    content = open('./data/topics_2017').read()
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

# Submit Functions
def load_submit_log():
    submit_log = {}
    for query in query_list:
        submit_log[query.id] = {}
        for day in range(1, day_span + 1):
            submit_log[query.id][day] = []

    for line in open(submit_log_file_path):
        t = line.strip().split('\t')
        qid, day = t[0], int(t[1])
        tweet = AdvancedTweet(t[2], t[3], t[4], t[5])
        submit_log[qid][day].append(tweet)
    return submit_log
submit_log = load_submit_log()
print 'submit_log size: %d' % len(submit_log)

def save_submit_log(qid, day, tweet):
    df = open(submit_log_file_path, 'a')
    created_at    = tweet.created_at
    id_str        = tweet.id_str
    word_list_str = ' '.join(tweet.word_list)
    stem_list_str = ' '.join(tweet.stem_list)
    df.write('%s\t%d\t%s\t%s\t%s\t%s\n' % (qid, day, created_at, id_str, word_list_str, stem_list_str))
    df.close()

def post_submit(qid, tid):
    try:
        conn=MySQLdb.connect(host='localhost',user='',passwd='',db='trec17',port=3306)
        cur=conn.cursor()
        cur.execute('INSERT INTO submit (qid, tid, client_id) VALUES (%s, %s, %s)', [qid, tid, client_id])
        conn.commit()
        cur.close()
        conn.close()
    except Exception, e:
        print e

######################################################### Define Score Function
def sim_q_t(query, tweet):
    return dir_score(query, tweet, corpus_dict, mu)
    
def sim_t_t(tweet_1, tweet_2):
    return sym_dir_score(tweet_1, tweet_2, corpus_dict, mu)

#########################################################
def run(query, tweet):
    dt = datetime.datetime.strptime(tweet.created_at, "%a %b %d %H:%M:%S +0000 %Y")
    curday = date2day(dt.month, dt.day)
    # submitting rate limitation
    if len(submit_log[query.id][curday]) >= 10: return
    # compute relevance score
    rel_score = sim_q_t(query, tweet)
    if rel_score > rel_thr:
        # find max redundance score
        max_red_score = 0
        for day in range(1, curday + 1):
            for submited_tweet in submit_log[query.id][day]:
                red_score = sim_t_t(tweet, submited_tweet)
                max_red_score = max(max_red_score, red_score)
        if max_red_score < red_thr:
            submit_log[query.id][curday].append(tweet)
            save_submit_log(query.id, curday, tweet)
            post_submit(query.id, tweet.id)
#########################################################

# query-tweet word overlap filtering
def is_overlap(query, tweet):
    for stem in query.stem_distri:
        if stem in tweet.stem_distri:
            return True
    return False

# another entrance of tweet processing
def tweet_handle(tweet):
    for query in query_list:
        if is_overlap(query, tweet):
            run(query, tweet)

# all stem filtering
def is_quick_filtered(tweet):
    for stem in tweet.stem_distri:
        if stem in all_stem_set:
            return False
    return True

# the entrance of tweet processing
def row_handle(created_at, id_str, word_list_str, stem_list_str):
    dt = datetime.datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
    curday = date2day(dt.month, dt.day)
    # date filtering
    if curday >= 1 and curday <= 10:
        tweet = AdvancedTweet(created_at, id_str, word_list_str, stem_list_str)
        if not is_quick_filtered(tweet):
           tweet_handle(tweet)

def main():
    try:
        conn = MySQLdb.connect(host   = 'localhost',
                               user   = '',
                               passwd = '',
                               db     = 'trec17',
                               port   = 3306)
        cur=conn.cursor()

        cur.execute('SELECT * FROM preprocess WHERE is_process = 0 limit 1000')
        rows = cur.fetchall()
        for row in rows:
            id            = row[0]
            created_at    = row[1]
            id_str        = row[2] 
            word_list_str = row[3]
            stem_list_str = row[4]

            # process tweets
            row_handle(created_at, id_str, word_list_str, stem_list_str)
            cur.execute('UPDATE preprocess SET is_process = 1 WHERE id = %d' % id)
        conn.commit()
            
        cur.close()
        conn.close()
            
        print 'process %d rows' % len(rows)
    except Exception, e:
        print e            

if __name__ == "__main__":

    # do the filtering every 10s.
    while True:
        print 'main ...'
        main()
        
        print 'sleep 10 seconds ...'
        time.sleep(10)


