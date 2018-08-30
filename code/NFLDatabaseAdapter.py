
import pymongo
import datetime as dt
from pprint import pprint
import pandas as pd
from helper_functions import clean_up_text
import twython as twy
import time
from operator import itemgetter
from datetime import datetime
from pymongo.errors import BulkWriteError


class NFLDatabaseAdapter(object):

    #static that holds the max data time

    def __init__(self):

        try:
            self.db_conn = pymongo.MongoClient()
            self.db = self.db_conn.NFL
            print("DB Connected successfully!!!")
        except pymongo.errors.ConnectionFailure as e:
            print("Could not connect to MongoDB: %s" % e)


        #Give access to all the collections
        self.tweets_coll = self.db.Tweets
        self.team_coll = self.db.Teams
        self.players_coll = self.db.Players
        self.date_ranges={"2014": [dt.datetime(2014 ,8 ,31), dt.datetime(2015,2,11)],
                          "2015": [dt.datetime(2015, 8, 31), dt.datetime(2016, 2, 11)],
                          "2016": [dt.datetime(2016, 8, 31), dt.datetime(2017, 2, 11)],
                          "2017": [dt.datetime(2017, 8, 31), dt.datetime(2018, 2, 11)],
                          "2018": [dt.datetime(2018, 8, 31), dt.datetime(2019, 2, 11)]}



    def _getRosterTwitterIds(self,mascot,year):
        roster_key = year + "_roster"
        roster = self.team_coll.find_one({"_id": mascot})[roster_key]
        ids = []
        for player in roster:
            id = player[1]
            if not id == "":
                ids.append(id)
        return ids
    def _getTeamTwitterIds(self,mascot):
        rosters = self.team_coll.find_one({"_id": mascot})
        ids = []
        for key in rosters.keys():
            if "roster" in key:
                for player in rosters[key]:
                    id = player[1]
                    if not id == "":
                        ids.append(id)
        return ids

    def export_tweet_cursor_to_csv(self,cursor,filename,no_id=False):

        # Expand the cursor and construct the DataFrame
                df = pd.DataFrame(list(cursor))
                # print(df)
                new_text = [clean_up_text(text) for text in df["full_text"]]
                df["full_text"] = new_text
                cols = ['screen_name', 'in_reply_to_screen_name', 'mentions', 'full_text']
                df.to_csv(filename, index=False, columns=cols)

    def export_teams_coll_to_csv(self, filename):

        # Expand the cursor and construct the DataFrame
        cursor = self.team_coll.find()
        rosters = list(cursor)
        with open(filename, "w") as f:
            for roster in rosters:
                for key in roster.keys():
                    if "roster" in key:
                        year = key[0:4]
                        mascot = roster['_id']
                        players = roster[key]
                        for player in players:
                            f.write(year + "," + mascot + "," + player[0] + "\n")

    def getTweetsByRosterYear(self,mascot,year,limit_by_date=True,verbose=False,ret_cursor=True):
        ids = self._getRosterTwitterIds(mascot, year)

        if verbose:
            ("Found "+str(len(ids))+" players for the "+mascot+" roster year "+year)
        pid_list = []
        for pid in ids:
            if verbose:
                print(pid)
            pid_list.append({"player_twitter_id":pid})
        query_limiter={"$or":pid_list}
        if limit_by_date:
            cursor = self._limit_tweets_to_date_range(start=self.date_ranges[year][0],end=self.date_ranges[year][1],query_limiter=query_limiter,verbose=verbose)
        else:
            cursor=self.tweets_coll.find(query_limiter)
        #if cursor == None:
        #    if verbose:
        #        print(self.players_coll.find_one({"_id":pid})["name"] +" did not tweet during the "+str(year)+" season.")
        #        return None
        #else:
        if ret_cursor:
            return cursor
        else:
            return pd.DataFrame(list(cursor))


    def getTweetsByTeam(self,mascot,verbose=False,ret_cursor=True):
        ids = self._getTeamTwitterIds(mascot)
        if verbose:
            ("Found "+str(len(ids))+" players for the "+mascot+" in all years.")
        pid_list = []
        for pid in ids:
            if verbose:
                print(pid)
            pid_list.append({"player_twitter_id":pid})
        query_limiter={"$or":pid_list}
        cursor=self.tweets_coll.find(query_limiter)
        if ret_cursor:
            return cursor
        else:
            return pd.DataFrame(list(cursor))




    def getTweetsByUser(self,screen_name=None,id=None,year=None,ret_cursor=True,verbose=False):

        if screen_name==None and id==None:
            print("ERROR: Must specify a user.")
            return None

        if id:
            query = {"player_twitter_id": id}
        else:
            query = {"screen_name":screen_name}

        if year:
            cursor = self._limit_tweets_to_date_range(start=self.date_ranges[year][0], end=self.date_ranges[year][1],
                                                      query_limiter=query, verbose=verbose)
        else:
            cursor = self.tweets_coll.find(query)
        if ret_cursor:
            return cursor
        else:
            return pd.DataFrame(list(cursor))



    def _limit_tweets_to_date_range(self,start=None,end=None,query_limiter=None,verbose=False):
        #start needs to be a datetime object
        #start = dt.strptime("18/01/13 18:30", "%d/%m/%y %H:%M")

        if start and not end:
            query = {'created_at': {"$gte": start}}

        elif not start and end:
            query={'created_at': {"$lte": end}}
        elif not start and not end:
            query = {}
        else:
            query={'created_at': {"$gte": start,"$lte": end}}
        if query_limiter:
            query.update(query_limiter)
        if verbose:
            pprint(query)
        cursor = self.tweets_coll.find(query)
        if verbose:
            print("Found "+str(cursor.count())+" tweets for this query.")
        return cursor

    def _get_most_recent_tweet(self,screen_name=None, id_number=None):
        if screen_name==None and id_number==None:
            print("ERROR: Must specify a user.")
            return None

        if id_number:
            query = {"player_twitter_id": id_number}
        else:
            query = {"screen_name":screen_name}

        return list(self.tweets_coll.find(query).sort([("created_at", 1)]))[-1]

    def _tweet_to_dict(self,tweet):
        # This function processes the raw JSON response, filters out unwanted fields and returns a dictionary
        data = {}
        # Can use 'id' which is an int64 or id_str which is identical but a string.
        data['_id'] = tweet['id_str']
        # UTC time when this Tweet was created
        data['created_at'] = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

        data['screen_name'] = tweet['user']['screen_name']
        data['player_twitter_id'] = tweet["user"]["id_str"]
        data['player_name'] = tweet['user']['name']
        data['user_loc'] = tweet['user']['location']
        data['geo'] = tweet['geo']
        # Indiciated the machine detected BCP 47 language identifier.
        data['language'] = tweet['lang']
        # Full unparsed tweet text
        data['full_text'] = tweet['full_text']
        # Parsed out hashtags
        data['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
        data['mentions'] = [mention['screen_name'] for mention in tweet['entities']['user_mentions']]
        if "extended_entities" in tweet.keys():
            data["media"] = tweet["extended_entities"]["media"]
        elif "media" in tweet["entities"]:
            data["media"] = tweet["entities"]["media"]
        data['in_reply_to_screen_name'] = tweet['in_reply_to_screen_name']
        return data



    def _tweet_overlap_exists(self,most_recent_known_id,new_tweet_block):

        temp = [self._tweet_to_dict(tweet) for tweet in new_tweet_block]
        new_tweets_sorted = sorted(temp, key=itemgetter('created_at'), reverse=False)
        last_known_tweet_time  = self.tweets_coll.find_one({"_id":most_recent_known_id})["created_at"]
        last_new_tweet_time = new_tweets_sorted[-1]["created_at"]
        print("known",last_known_tweet_time)
        print("first",new_tweets_sorted[0]["created_at"])
        print("last",last_new_tweet_time)
        print("***************")
        if last_known_tweet_time> last_new_tweet_time:
            print("Already have these tweets..skipping.")
            return 199,new_tweets_sorted
        new_start_id = -1
        for idx, t in enumerate(new_tweets_sorted):
            # print(idx,t["created_at"])
            if most_recent_known_id == t["_id"]:
                new_start_id = idx

        return new_start_id,new_tweets_sorted



    def _get_new_tweets(self,twython_conn,screen_name=None, id_number=None):
        """
        Gets the most recent (at most) 3200 tweets.
        :param twython_conn: A connection to the twython API
        :param screen_name: The users screen name/twitter handle you wish to update
        :param id_number: The users twitter id you wish to update
        :return: A list of tweets.
        """
        tweets = []
        done = False
        newUser = False
        try:
            if screen_name:
                print("Starting "+screen_name)
                if self.tweets_coll.find({"screen_name": screen_name}).count() == 0:
                    print("This is a new screen_name ...")
                    newUser = True
                else:
                    print("Tweets for this user found in DB.")
            elif id_number:
                print("Starting "+id_number)
                if self.tweets_coll.find({"player_twitter_id": id_number}).count() == 0:
                    print("This is a new ID ...")
                    newUser = True
                else:
                    print("Tweets for this user found in DB.")

            if screen_name is not None or id_number is not None:
                maxID =None
                while not done:
                    if maxID==None:
                        new_tweet_block = twython_conn.get_user_timeline(screen_name=screen_name,user_id=id_number, count=200, tweet_mode="extended")
                    else:
                        new_tweet_block = twython_conn.get_user_timeline(screen_name=screen_name,user_id=id_number, count=200, tweet_mode="extended", max_id=maxID-1)

                    if not newUser:
                        most_recent_known_id = self._get_most_recent_tweet(screen_name=screen_name, id_number=id_number)["_id"]

                        idx,new_tweets_sorted = self._tweet_overlap_exists(most_recent_known_id=most_recent_known_id, new_tweet_block=new_tweet_block)
                        if not idx == -1:
                            temp = new_tweets_sorted[idx + 1:]
                            print("Found ",len(temp))
                            done = True
                        else:
                            done = False
                            temp = [self._tweet_to_dict(tweet) for tweet in new_tweet_block]
                    else:
                        temp = [self._tweet_to_dict(tweet) for tweet in new_tweet_block]
                    if new_tweet_block == []:
                        done = True
                        print("Collected..."+str(len(tweets))+" tweets.")
                    else:
                        # i += 1
                        maxID = new_tweet_block[- 1]['id']
                        tweets.extend(temp)
                        time.sleep(1)
            else:
                print("Must provide user info.")

        except (twy.TwythonError, twy.TwythonAuthError, twy.TwythonRateLimitError) as e:
            print(e)

        return tweets
    def updateUserTweets(self,twython_conn,screen_name=None, id_number=None):
        new_tweets = self._get_new_tweets(twython_conn=twython_conn,screen_name=screen_name,id_number=id_number)
        if len(new_tweets)>0:
            #print("would insert ...", new_tweets[0], "and ", len(new_tweets), "more tweets")
            try:
                self.tweets_coll.insert_many(new_tweets)
            except BulkWriteError as bwe:
                print(bwe.details)
                # you can also take this component and do more analysis
                # werrors = bwe.details['writeErrors']
                raise
        else:
            print("This user does not need to be updated.")


    def updateTeam(self,mascot):
        keyFile = open('api_key.keys', 'r')
        consumer_key = keyFile.readline().strip()
        consumer_secret = keyFile.readline().strip()
        OAUTH_TOKEN = keyFile.readline().strip()
        OAUTH_TOKEN_SECRET = keyFile.readline().strip()
        keyFile.close()
        twython_conn = twy.Twython(consumer_key, consumer_secret, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        ids =self._getTeamTwitterIds(mascot)
        print("Starting to update and insert..."+mascot)
        for id in ids:
            self.updateUserTweets(twython_conn=twython_conn,
                                  id_number=id)

