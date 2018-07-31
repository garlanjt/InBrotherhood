
import twython as twy
import tweepy
import os
import time
import requests
from nltk.corpus import stopwords
import pymongo
from datetime import datetime
from collections import Counter
import json
import re
import string
from wordcloud import WordCloud
import numpy as np
import random
from PIL import Image
from NFLDatabaseAdapter import NFLDatabaseAdapter
from os import path




def read_user_tweet_cache(username):
    inputfile = "../data/jsondump/"+username+".json"
    with open(inputfile,"r") as f:
        tweet_strs=f.readlines()
    f.close()
    return [json.loads(x.strip()) for x in tweet_strs]








def main():
    start = datetime(2017 ,8 ,31)
    end =datetime(2018,2,11)
    start = None
    end = None

    player= "BenGarland63"


    db = NFLDatabaseAdapter()


    #cursor = limit_user_tweets_to_date_range(DB=tweets,
    #                            screen_name=player,
    #                            start=start,
    #                            end =end)
    #cursor = limit_user_tweets_to_date_range(DB=tweets,
    #                            screen_name=player,
    #                            start=start,
    #                            end =end)


    #For Poster

    #cursor = limit_tweets_to_date_range(DB=tweets,
    #                                    start=start,
    #                                    end=end)
    make_WordCloud(cursor, "falcons-in-falcon-", 100, both = True)


    "brother","bro","gang"


    #count_hashtags(cursor)
    #for tweet in cursor:
    #    print(tweet['hashtags'])
    unique_hashtags(cursor)




main()