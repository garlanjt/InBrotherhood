import twython as twy
import tweepy
import os
import time
import requests
import json
import pymongo
from datetime import datetime
from dbHandler import getRosterTwitterIds
#Global Flags#
CACHE_JSON = False
#####



def get_screenname_ids(db,screenname):
    ids = []
    cursor=db.find({"screen_name":screenname},{})
    for record in cursor:
        ids.append(record['_id'])
    return ids


###Cacheing methods################################


def read_user_tweet_cache(username):
    inputfile = "../data/jsondump/"+username+".json"
    with open(inputfile,"r") as f:
        tweet_strs=f.readlines()
    f.close()
    return [json.loads(x.strip()) for x in tweet_strs]

def cache_tweet(to_cache,username):
    out_file_name = "../data/jsondump/"+username+".json"
    if type(to_cache) ==dict:
        with open(out_file_name, 'a') as outfile:
            outfile.write(json.dumps(to_cache) + "\n")
            outfile.close()
    elif type(to_cache) == list:
        with open(out_file_name, 'a') as outfile:
            for tweet in to_cache:
                outfile.write(json.dumps(tweet) + "\n")
            outfile.close()
    else:
        print(to_cache,"unknown cache type")

        #################################################

def cache_tweets(to_cache_list,username):
    for tweet in to_cache_list:
        cache_tweet(tweet, username)

# def collect_user_timeline(API,tweets_col,username):
#
#     user_timeline =getUserTweets(API, screen_name=username)
#     if CACHE_JSON:
#         print("starting to cache tweets",username)
#         cache_tweets(user_timeline,username)
#         print("finished caching tweets",username)
#     #<todo>This is hacky to avoid double insertion, an upsert may be better here.
#     ids =get_screenname_ids(tweets_col,username)
#     processed_timeline = []
#     for tweet in user_timeline:
#         if tweet['id_str'] not in ids:
#             #Parse the tweet
#             tweet_dict = tweet_to_dict(tweet)
#             processed_timeline.append(tweet_dict)
#
#             #Cache the tweet in case we fucked up
#             #if CACHE_JSON:
#             #    cache_tweet(tweet,username)
#         #else:
#         #    print("skipping...")
#         #    print(tweet)
#     #print("Done inserting "+username+" into DB.")
#     return processed_timeline




def process_raw_tweets(tweets,id):

    if CACHE_JSON:
        print("starting to cache tweets",id)
        cache_tweets(tweets,id)
        print("finished caching tweets",id)
    #<todo>This is hacky to avoid double insertion, an upsert may be better here.
    processed_timeline = []
    for tweet in tweets:
        tweet_dict = tweet_to_dict(tweet)
        processed_timeline.append(tweet_dict)
    return processed_timeline


def tweet_to_dict(tweet):
    # This function processes the raw JSON response, filters out unwanted fields and returns a dictionary
    data ={}
    #Can use 'id' which is an int64 or id_str which is identical but a string.
    data['_id'] = tweet['id_str']
    # UTC time when this Tweet was created
    data['created_at'] = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

    data['screen_name'] = tweet['user']['screen_name']
    data['player_twitter_id'] = tweet["user"]["id_str"]
    data['player_name'] = tweet['user']['name']
    data['user_loc'] = tweet['user']['location']
    data['geo'] = tweet['geo']
    #Indiciated the machine detected BCP 47 language identifier.
    data['language'] = tweet['lang']
    #Full unparsed tweet text
    data['full_text'] = tweet['full_text']
    #Parsed out hashtags
    data['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    data['mentions'] = [mention['screen_name'] for mention in tweet['entities']['user_mentions']]
    if "extended_entities" in tweet.keys():
        data["media"] = tweet["extended_entities"]["media"]
    elif "media" in tweet["entities"]:
        data["media"] =tweet["entities"]["media"]
    data['in_reply_to_screen_name']= tweet['in_reply_to_screen_name']
    return data






# Below function will get tweets based on screen_name or ID of user.  If both are given, will use ID.
def getUserTweets(twython_conn,screen_name=None, id_number=None):
    tweets = []
    done = False
    first_call = True
    try:  # This try-except will catch Twython errors
        if screen_name is not None:
            #<todo> this skips most recent tweet

            maxID = twython_conn.get_user_timeline(screen_name=[screen_name], count=1)[0]['id']  # Current max ID is ID of newest tweet

            while not done:
                temp = twython_conn.get_user_timeline(screen_name=[screen_name], tweet_mode = "extended", count=200, max_id=maxID - 1)  # Subtract 1 so that you do not loop through the last tweet, constantly downloading it until i equals 15.
                if temp == []:
                    #i =16 #This ensures you only request <3200 tweets
                    print("All tweets for "+ screen_name +" collected.")
                    done = True
                else:
                    #i +=1
                    maxID = temp[len(temp) - 1]['id']
                    tweets.extend(temp)
                    time.sleep(1)

        if id_number is not None:
            #maxID = twython_conn.get_user_timeline(user_id=[id_number], count=1)[0]['id']  # Current max ID is ID of newest tweet
            #for i in range(0, 16):  # Cannot return more than 3200 tweets; 200 at a time equals 16 cycles
            while not done:
                if first_call:
                    temp = twython_conn.get_user_timeline(user_id=[id_number], count=200, tweet_mode = "extended")  # Subtract 1 so that you do not loop through the last tweet, constantly downloading it until i equals 15.
                    first_call=False
                else:
                    temp = twython_conn.get_user_timeline(user_id=[id_number], count=200, tweet_mode = "extended", max_id=maxID - 1)  # Subtract 1 so that you do not loop through the last tweet, constantly downloading it until i equals 15.

                if temp == []:
                    done = True
                    print("All tweets for " + id_number + " collected.")
                else:
                    #i += 1
                    maxID = temp[- 1]['id']
                    tweets.extend(temp)
                    time.sleep(1)


    except (twy.TwythonError, twy.TwythonAuthError, twy.TwythonRateLimitError) as e:
        print(e)


    return tweets




def get_player_handles(roster):
    with open(roster) as f:
        content = f.readlines()
        r_h = [x.strip() for x in content]
    return r_h

def insert_player_tweets(twython_api, tweets_coll, player_handle):
    # Go get the tweets we are missing.
    processed_timeline = collect_user_timeline(twython_api, tweets_coll, player_handle)
    if len(processed_timeline)>0:
        tweets_coll.insert_many(processed_timeline)



# def insert_team_tweets(team_to_insert,teams,twython_api,tweets_coll):
#     #team_to_insert = "AtlantaFalcons"
#
#     #processedFile = "../logs/processed/" + team_to_insert + "CompletedPlayers.txt"
#     player_handles = teams.find_one({"_id": team_to_insert})["known_players"]
#     for player_handle in player_handles:
#         if tweets_coll.find({"screen_name": player_handle}).count() == 0:
#
#             print("Starting ",player_handle)
#             insert_player_tweets(twython_api, tweets_coll, player_handle)
#             print("Going to sleep")
#             time.sleep(15)
#         else:
#             print("Skipping player",player_handle)

def init_insert_roster_tweets(db,twython_conn,mascot,year):
    # Connect to team and tweet collections
    player_collection = db.Players
    team_collection = db.Teams
    tweet_collection =db.Tweets
    ids =getRosterTwitterIds(team_collection=team_collection,mascot=mascot,year=year)
    for p_id in ids:
        curr_player = player_collection.find_one({"_id": p_id})
        print("Starting "+str(curr_player["name"])+" "+str(curr_player["handle"])+" "+str(p_id))
        if tweet_collection.find({"player_twitter_id": p_id}).count() == 0:

            if not curr_player["private"]:
                print("Collecting... "+curr_player["name"])
                #pull Tweets down from Twitter
                tweets=getUserTweets(twython_conn, id_number=p_id)
                if len(tweets)>0:
                    print("Discovered "+str(len(tweets))+" tweets.")
                    #Process the tweets into what we care about
                    tweets=process_raw_tweets(tweets, id)
                    #Insert them into the database
                    tweet_collection.insert_many(tweets)
                    print("Going to sleep")
                    time.sleep(15)
                else:
                    print("No tweets discovered for "+str(curr_player["name"]))

        else:
            print("Skipped "+str(curr_player["name"])+" already collected")

def main():
        #roster_file = "../data/falcons_handles.csv"
        keyFile = open('api_key.keys', 'r')
        consumer_key = keyFile.readline().strip()
        consumer_secret = keyFile.readline().strip()
        OAUTH_TOKEN = keyFile.readline().strip()
        OAUTH_TOKEN_SECRET = keyFile.readline().strip()
        keyFile.close()


        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
        api = tweepy.API(auth)

        twython_api = twy.Twython(consumer_key, consumer_secret, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        try:
            db_conn = pymongo.MongoClient()
            print "DB Connected successfully!!!"
        except pymongo.errors.ConnectionFailure, e:
            print "Could not connect to MongoDB: %s" % e

        db = db_conn.NFL
        #player="BenGarland63"

        #####This is for testing purposes and should be removed!!!!!!!
        #tweets.delete_many({})



        #for team_to_insert in ["AtlantaFalcons","Patriots","Seahawks","Broncos","dallascowboys"]:
        for mascot in ["Falcons","Patriots","Broncos"]:
            for year in range(2014,2018):
                init_insert_roster_tweets(db=db,twython_conn=twython_api,mascot=mascot,year=str(year))





main()