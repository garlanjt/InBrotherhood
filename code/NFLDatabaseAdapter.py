
import pymongo
import datetime as dt
from pprint import pprint
import pandas as pd
from helper_functions import clean_up_text

class NFLDatabaseAdapter(object):

    #static that holds the max data time

    def __init__(self):

        try:
            self.db_conn = pymongo.MongoClient()
            self.db = self.db_conn.NFL
            print("DB Connected successfully!!!"_
        except pymongo.errors.ConnectionFailure, e:
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





