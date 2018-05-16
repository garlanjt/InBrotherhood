import twython as twy
import tweepy
import os
import time
import requests
import json
import pymongo
from datetime import datetime

#Global Flags#
CACHE_JSON = True
#####



def get_screenname_ids(db,screenname):
    ids = []
    cur=db.find({"screen_name":screenname},{})
    for record in cur:
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

def collect_user_timeline(API,tweets_col,username):

    user_timeline =getUserTweets(API, screen_name=username)
    #<todo>This is hacky to avoid double insertion, an upsert may be better here.
    ids =get_screenname_ids(tweets_col,username)
    processed_timeline = []
    for tweet in user_timeline:

        if tweet['id_str'] not in ids:
            #Parse the tweet
            tweet_dict = tweet_to_dict(tweet)
            processed_timeline.append(tweet_dict)
            #Cache the tweet in case we fucked up
            #if CACHE_JSON:
            #    cache_tweet(tweet,username)
        #else:
        #    print("skipping...")
        #    print(tweet)
    #print("Done inserting "+username+" into DB.")
    return processed_timeline



def tweet_to_dict(tweet):
    # This function processes the raw JSON response, filters out unwanted fields and returns a dictionary
    data ={}
    #Can use 'id' which is an int64 or id_str which is identical but a string.
    data['_id'] = tweet['id_str']
    # UTC time when this Tweet was created
    data['created_at'] = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

    data['screen_name'] = tweet['user']['screen_name']

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
def getUserTweets(connection,screen_name=None, id_number=None):
    tweets = []
    try:  # This try-except will catch Twython errors
        if screen_name is not None:
            maxID = connection.get_user_timeline(screen_name=[screen_name], count=1)[0]['id']  # Current max ID is ID of newest tweet
            #try:  # try-catch to avoid error message when all tweets are complete
            i =0
            while i<16:
                temp = connection.get_user_timeline(screen_name=[screen_name], tweet_mode = "extended", count=200, max_id=maxID - 1)  # Subtract 1 so that you do not loop through the last tweet, constantly downloading it until i equals 15.
                if temp == []:
                    i =16 #This ensures you only request <3200 tweets
                    print("All tweets for "+ screen_name +" collected.")
                else:
                    i +=1
                    maxID = temp[len(temp) - 1]['id']
                    tweets.extend(temp)
                    time.sleep(3)

        if id_number is not None:
            maxID = connection.get_user_timeline(user_id=[id_number], count=1)[0]['id']  # Current max ID is ID of newest tweet
            for i in range(0, 16):  # Cannot return more than 3200 tweets; 200 at a time equals 16 cycles
                temp = connection.get_user_timeline(user_id=[id_number], count=200, tweet_mode = "extended", max_id=maxID - 1)  # Subtract 1 so that you do not loop through the last tweet, constantly downloading it until i equals 15.
                if temp == []:
                    i = 16  # This ensures you only request <3200 tweets
                    print("All tweets for " + id_number + " collected.")
                else:
                    i += 1
                    maxID = temp[len(temp) - 1]['id']
                    tweets.extend(temp)
                    time.sleep(3)


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
        #if x[""]
    #else:
    #outF.write(player_handle)
    #outF.write("\n")

    if CACHE_JSON:
        print("starting to cache tweets")
        cache_tweets(processed_timeline,player_handle)
        print("finished caching tweets")
#    cache_tweet(tweet,username)
    #time.sleep(30)


def insert_team_tweets(team_to_insert,teams,twython_api,tweets_coll):
    #team_to_insert = "AtlantaFalcons"

    #processedFile = "../logs/processed/" + team_to_insert + "CompletedPlayers.txt"
    player_handles = teams.find_one({"_id": team_to_insert})["known_players"]

    #if os._exists(processedFile):
    #    with open(processedFile, "r") as f:
    #        done = f.readlines().strip()
    #else:
    #    done = set([])
    #print("Skipping ... ",done)
    #players_to_process = set(player_handles) - set(done)

    #outF = open(processedFile, "a")

    for player_handle in player_handles:
        if tweets_coll.find({"screen_name": player_handle}).count() == 0:

            print("Starting ",player_handle)
            insert_player_tweets(twython_api, tweets_coll, player_handle)
            print("Going to sleep")
            time.sleep(15)
        else:
            print("Skipping player",player_handle)

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
        # Make/Connect to a collection of tweets
        tweets = db.Tweets
        #player="BenGarland63"

        #####This is for testing purposes and should be removed!!!!!!!
        #tweets.delete_many({})

        teams = db.Teams

        for team_to_insert in ["AtlantaFalcons","Patriots","Seahawks","Broncos","dallascowboys"]:
            insert_team_tweets(team_to_insert=team_to_insert,
                               teams= teams,
                               twython_api=twython_api,
                               weets_coll=tweets)

        #players =get_player_handles(roster_file)
        #for player in players:
        #    collect_user_timeline(twython_api, tweets, player)




main()