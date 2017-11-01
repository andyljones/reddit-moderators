#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  1 10:57:08 2017

@author: emg
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 18:01:01 2017

@author: emg
"""
# using code from http://brandonrose.org/clustering
import numpy as np
import pandas as pd
import nltk
import re
from sklearn import feature_extraction, externals, cluster, metrics, manifold
from bigquery import *

ex = example()

comments = [comment for comment in ex['body'] if comment != '[deleted]']

stopwords = nltk.corpus.stopwords.words('english')
stemmer = nltk.stem.snowball.SnowballStemmer("english")

def tokenize_and_stem(text):
    tokens = [token.lower() for sent in nltk.sent_tokenize(text) for token in nltk.word_tokenize(sent)]
    stopless = [token for token in tokens if token not in stopwords]
    words = [word for word in stopless if re.search('[a-zA-Z]', word)]
    stems = [stemmer.stem(word) for word in words]
    return stems

def stem_comment_matrix():
    all_stems = []
    bows = []
    for comment in comments:
        stems = tokenize_and_stem(comment)
        all_stems.extend(stems)
        bows.append(stems)
    
    incidence = []
    for comment in bows:
        presence = []
        for stem in all_stems:
            if stem in comment:
                presence.append(1)
            else:
                presence.append(0)
        incidence.append(presence)
    
    incidence_matrix = pd.DataFrame(incidence, columns=all_stems)
    return incidence_matrix

m = stem_comment_matrix()
m.head()
