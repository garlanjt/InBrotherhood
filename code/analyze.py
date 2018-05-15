
import twython as twy
import tweepy
import os
import time
import requests
import pymongo
from datetime import datetime
from collections import Counter
import json




def read_user_tweet_cache(username):
    inputfile = "../data/jsondump/"+username+".json"
    with open(inputfile,"r") as f:
        tweet_strs=f.readlines()
    f.close()
    return [json.loads(x.strip()) for x in tweet_strs]





def limit_user_tweets_to_date_range(DB,screen_name,start=None,end=None):
    #start needs to be a datetime object
    #start = datetime.strptime("18/01/13 18:30", "%d/%m/%y %H:%M")

    if start and not end:
        cursor = DB.find({'created_at': {"$gte": start},"screen_name":screen_name})
        #for tweet in cursor:
        #    print(tweet)
    elif not start and end:
        cursor = DB.find({'created_at': {"$lte": end},"screen_name":screen_name})
        #for tweet in cursor:
        #    print(tweet)
    elif not start and not end:
        cursor = DB.find({ "screen_name": screen_name})

    else:
        cursor = DB.find({'created_at': {"$gte": start,"$lte": end},"screen_name":screen_name})
        print(DB.find({'created_at': {"$gte": start,"$lte": end},"screen_name":screen_name}).count())
        ##for tweet in cursor:
        #    print(tweet)
    return cursor

def unique_hashtags(tweets):
    hashtags = []
    for tweet in tweets:
        for hashtag in tweet['hashtags']:
            hashtags.append(hashtag.lower())
    print(len(set(hashtags)))
    #counts = Counter(hashtags)
    #print(counts)
def count_hashtags(tweets):
    hashtags = []
    for tweet in tweets:
        for hashtag in tweet['hashtags']:
            hashtags.append(hashtag.lower())
    counts = Counter(hashtags)
    print(counts)




def main():
    start = datetime(2017 ,8 ,31)
    end =datetime(2018,2,11)
    start = None
    end = None

    player= "BenGarland63"

    #Connect to the DB
    try:
        db_conn = pymongo.MongoClient()
        print "DB Connected successfully!!!"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e

    db = db_conn.NFLPlayerTweets
    # Make/Connect to a collection of tweets
    tweets = db.Tweets



    cursor = limit_user_tweets_to_date_range(DB=tweets,
                                screen_name=player,
                                start=start,
                                end =end)


    #count_hashtags(cursor)
    #for tweet in cursor:
    #    print(tweet['hashtags'])
    unique_hashtags(cursor)




main()