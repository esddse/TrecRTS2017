import math
import numpy as np

##################################################
def kl_normalize(score):
    max_v = 0.0
    min_v = -20.0
    if score > max_v: score = max_v
    if score < min_v: score = min_v
    return (score - min_v) / (max_v - min_v)

def kl_jm(distribution_q, distribution_t, distribution_c, lamda):
    res = 0.0
    flag = False
    for key in distribution_q:
        smooth = 0
        if key in distribution_t: 
            smooth += (1 - lamda) * distribution_t[key]
        if key in distribution_c:
            smooth += lamda * distribution_c[key]
        if smooth != 0: 
            res += distribution_q[key] * math.log(smooth)
            flag = True
    if flag: return res
    return -999999.0

def kl_dirichlet(distribution_q, distribution_t, distribution_c, mu, t_len):
    alpha = float(mu) / (t_len + mu)
    return kl_jm(distribution_q, distribution_t, distribution_c, alpha)

def cosine(tf_q, tf_t, idf_dict):
    dot = .0
    q_len = .0
    t_len = .0
    tf_idf_q = {}
    tf_idf_t = {}

    for term in tf_q:
        if term in idf_dict:
            tf_idf_q[term] = tf_q[term] * idf_dict[term]
            q_len += tf_idf_q[term] ** 2
    for term in tf_t:
        if term in idf_dict:
            tf_idf_t[term] = tf_t[term] * idf_dict[term]
            t_len += tf_idf_t[term] ** 2
            if term in tf_idf_q:
                dot += tf_idf_q[term] * tf_idf_t[term]

    return dot / (q_len ** 0.5 * t_len ** 0.5)


##################################################
def jm_score(query, tweet, corpus_dict, lamda=0.5):
    return kl_normalize(kl_jm(query.stem_distri, tweet.stem_distri, corpus_dict, lamda))

def sym_jm_score(tweet_1, tweet_2, corpus_dict, lamda=0.5):
    score_1 = jm_score(tweet_1, tweet_2, corpus_dict, lamda)
    score_2 = jm_score(tweet_2, tweet_1, corpus_dict, lamda)
    return (score_1 + score_2) / 2.0

def dir_score(query, tweet, corpus_dict, mu=100):
    return kl_normalize(kl_dirichlet(query.stem_distri, tweet.stem_distri, corpus_dict, mu, len(tweet.stem_list)))

def sym_dir_score(tweet_1, tweet_2, corpus_dict, mu=100):
    score_1 = dir_score(tweet_1, tweet_2, corpus_dict, mu)
    score_2 = dir_score(tweet_2, tweet_1, corpus_dict, mu)
    return (score_1 + score_2) / 2.0

def cosine_score(query, tweet, idf_dict):
    return cosine(query.expanded_tf, tweet.stem_tf, idf_dict)

def sym_cosine_score(tweet_1, tweet_2, idf_dict):
    return cosine(tweet_1.stem_tf, tweet_2.stem_tf, idf_dict)



