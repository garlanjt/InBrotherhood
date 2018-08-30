# Author: Alex Perrier <alexis.perrier@gmail.com>
# License: BSD 3 clause
# Python 2.7
'''
This script loads a gensim dictionary and associated corpus
and applies an LDA model.
The documents are timelines of a 'parent' Twitter account.
They are retrieved in their tokenized version from a MongoDb database.
See also the following blog posts
* http://alexperrier.github.io/jekyll/update/2015/09/04/topic-modeling-of-twitter-followers.html
* http://alexperrier.github.io/jekyll/update/2015/09/16/segmentation_twitter_timelines_lda_vs_lsa.html
'''
from gensim import corpora, models, similarities
from pymongo import MongoClient
from time import time
import numpy as np
import sys

teamname = sys.argv[1]

def connect(DB_NAME):
    client      = MongoClient()
    return client[DB_NAME]
'''
def get_documents():
    condition = {'has_tokens': True, 'is_included': True}
    #print(db.Tweets.find_one())
    tweets = db.Tweets.find(condition).sort("screen_name")
    tweets = db.Tweets.find()
    documents = [ { 'screen_name': tw['screen_name'], 'tokens': tw['tokens']}
                    for tw in tweets  ]
    return documents
'''
# Initialize Parameters
corpus_filename = teamname + '/corpus.mm'
dict_filename   = teamname + '/dictionary.dict'
lda_filename    = teamname + '/model.lda'
lda_params      = {'num_topics': 10, 'passes': 300, 'alpha': 0.001}

# Connect and get the documents
#db              = connect('NFL')
#documents       = get_documents()

#print("Corpus of %s documents" % len(documents))

# Load the corpus and Dictionary
corpus = corpora.MmCorpus(corpus_filename)
dictionary = corpora.Dictionary.load(dict_filename)

print("Running LDA with: %s  " % lda_params)
lda = models.LdaMulticore(corpus, id2word=dictionary,
                        num_topics=lda_params['num_topics'],
                        passes=lda_params['passes'],
                        alpha = lda_params['alpha'])
print()
lda.print_topics()
lda.save(lda_filename)
print("lda saved in %s " % lda_filename)
