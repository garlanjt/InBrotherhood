from __future__ import print_function
import langid
import logging
import nltk
import numpy as np
import re
import sys
import time
from collections import defaultdict
from gensim import corpora
from optparse import OptionParser
from pymongo import MongoClient
from string import digits
from NFLDatabaseAdapter import NFLDatabaseAdapter

# --------------------------------------
#  Database functions
# --------------------------------------

def connect(DB_NAME):
    client = MongoClient()
    return client[DB_NAME]

# Get the documents from the DB
'''
def get_timelines(parent):
    tweets = db.Tweets.find({'parent': parent})
    return tweets
'''

def filter_by_length_and_lang(percent = 25, lang = ['en','und']):
    tweets.rewind()
    # exclude 25% of documents with little content
    len_text = [ tw['len_text']  for tw in tweets
                    if 'len_text' in tw.keys() and tw['len_text'] > 0 ]
    #threshold  = np.percentile(len_text , percent)

    tweets.rewind()
    # filter on lang and
    documents = [{ 'screen_name': tw['screen_name'], 'text': tw['full_text']}
                    for tw in tweets
                    if ('language' in tw.keys())
                        and (tw['language'] in lang)

                ]
    # Keep documents in English or Undefined lang and with enough content
    return documents

# --------------------------------------
#  Clean documents functions
# --------------------------------------

def remove_urls(text):
    text = re.sub(r"(?:\@|http?\://)\S+", "", text)
    text = re.sub(r"(?:\@|https?\://)\S+", "", text)
    return text

def doc_rm_urls():
    return [ { 'screen_name': doc['screen_name'], 'text': remove_urls(doc['text']) }
                for doc in documents ]

def stop_words_list():
    '''
        A stop list specific to the observed timelines composed of noisy words
        This list would change for different set of timelines
    '''
    return ['amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
            'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via',
            'one','com','new','like','great','make','top','awesome','best',
            'good','wow','yes','say','yay','would','thanks','thank','going',
            'new','use','should','could','really','see','want','nice',
            'while','know','free','today','day','always','last','put','live',
            'week','went','wasn','was','used','ugh','try','kind', 'http','much',
            'need', 'next','app','ibm','appleevent','using']

def all_stopwords():
    '''
        Builds a stoplist composed of stopwords in several languages,
        tokens with one or 2 words and a manually created stoplist
    '''
    # tokens with 1 characters
    unigrams = [ w for doc in tokenized_documents for w in doc['tokens']
                    if len(w)==1]
    # tokens with 2 characters
    bigrams  = [ w for doc in tokenized_documents for w in doc['tokens']
                    if len(w)==2]

    # Compile global list of stopwords
    stoplist  = set(  nltk.corpus.stopwords.words("english")
                    + nltk.corpus.stopwords.words("french")
                    + nltk.corpus.stopwords.words("german")
                    + stop_words_list()
                    + unigrams + bigrams)
    return stoplist

# This returns a list of tokens / single words for each user
def tokenize_doc():
    '''
        Tokenizes the raw text of each document
    '''
    tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')
    return [   {  'screen_name': doc['screen_name'],
                   'tokens': tokenizer.tokenize(doc['text'].lower())
                }
                for doc in documents ]

def count_token():
    '''
        Calculates the number of occurence of each word across the whole corpus
    '''
    token_frequency = defaultdict(int)
    for doc in tokenized_documents:
        for token in doc['tokens']:
            token_frequency[token] += 1
    return token_frequency

def token_condition(token):
    '''
        Only keep a token that is not in the stoplist,
        and with frequency > 1 among all documents
    '''
    return  (token not in stoplist and len(token.strip(digits)) == len(token)
                        and token_frequency[token] > 1)

def keep_best_tokens():
    '''
        Removes all tokens that do not satistify a certain condition
    '''
    return [   {  'screen_name': doc['screen_name'],
                   'tokens': [ token for token in doc['tokens']
                                if token_condition(token) ]
                }
                for doc in tokenized_documents]

# ---------------------------------------------------------
#  Main
# ---------------------------------------------------------

print(__doc__)
# Display progress logs on stdout
logging.basicConfig(level=logging.INFO,
                    format='>>> %(asctime)s %(levelname)s %(message)s')

# ---------------------------------------------------------
#  parse commandline arguments
# ---------------------------------------------------------

op = OptionParser()
'''
op.add_option("-s", "--screen_name",
              dest="screen_name", type="string",
              help="Screen name of the parent Twitter account")
'''
op.add_option("-d", "--dbname", dest="dbname", default='twitter',
              help="Name of the MongDB collection, default: twitter")

op.add_option("--save_dict", dest="dict_filename", default=None,
              help="Set to the filename of the Dictionary you want to save")

op.add_option("--save_corpus", dest="corpus_filename", default=None,
              help="Set to the filename of the Corpus you want to save")
op.add_option("--team", dest="teamname", default=None,
              help="Specify a team name to run a team-specific LDA model")

# Initialize
(opts, args) = op.parse_args()
print(opts)

#screen_name  = opts.screen_name.lower()     # The parent twitter account

#  MongoDB connection
db = NFLDatabaseAdapter()
#tweets = get_timelines(screen_name)
team = opts.teamname
#tweets = db.getTweetsByRosterYear(mascot=team,year='2017',limit_by_date=False)
tweets = db.getTweetsByTeam(mascot=team)

# ---------------------------------------------------------
#  Documents / timelines selection and clean up
# ---------------------------------------------------------

# Keep 1st Quartile of documents by length and filter out non-English words
documents = filter_by_length_and_lang(25, ['en','und'])

# Remove urls from each document
documents = doc_rm_urls()

print("\nWe have " + str(len(documents)) + " documents in english ")
print()

# ---------------------------------------------------------
#  Tokenize documents
# ---------------------------------------------------------

# At this point tokenized_documents.keys() == ['user_id', 'tokens']
tokenized_documents = tokenize_doc()

token_frequency     = count_token()
stoplist            = all_stopwords()
tokenized_documents = keep_best_tokens()

# for visualization purposes only
for doc in tokenized_documents:
    doc['tokens'].sort()

# ---------------------------------------------------------
#  Save tokenized docs in database
# ---------------------------------------------------------
# We save the tokenized version of the raw text in the db
#tweets = db.tweets.find()
for twt in tweets:
    docs = [doc for doc in tokenized_documents
                if doc['screen_name'] == twt['screen_name']]
    if len(docs) == 1:
        # update existing document with tokens
        update = {  'tokens': docs[0]['tokens'],
                    'has_tokens': True,
                    'len_tokens': len(docs[0]['tokens'])
                }
    else:
        # the document was not tokenized, update the record accordingly
        update = { 'tokens': '', 'has_tokens': False, 'len_tokens':0 }

    # update the record
    res = db.tweets.update_one(
        {'_id':twt['_id']},
        { "$set":update }
    )

    if res.matched_count != 1:
        print("unable to update record: ",
                str(twt['screen_name']), str(twt['_id']), str(res.raw_result))

# How many documents were tokenized?
#tweets = db.tweets.find({'has_tokens': True})
#print("\nWe have %s tokenized documents in the database" % tweets.count())
#print()

# ---------------------------------------------------------
#  Dictionary and Corpus
# ---------------------------------------------------------

# build the dictionary
dictionary = corpora.Dictionary([doc['tokens'] for doc in tokenized_documents])
dictionary.compactify()

# We now have a dictionary with N unique tokens
print("Dictionary: ", end=' ')
print(dictionary)
print()

# and save the dictionary for future use
if opts.dict_filename is not None:
    print("dictionary saved in %s " % opts.dict_filename)
    dictionary.save(opts.dict_filename)

# Build the corpus: vectors with occurence of each word for each document
# and convert tokenized documents to vectors
corpus = [  dictionary.doc2bow(doc['tokens']) for doc in tokenized_documents]

# and save in Market Matrix format
if opts.corpus_filename is not None:
    print("corpus saved in %s " % opts.corpus_filename)
    corpora.MmCorpus.serialize(opts.corpus_filename, corpus)
