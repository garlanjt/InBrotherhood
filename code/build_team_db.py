import twython as twy
import tweepy
import os
import time
import requests
import json
import pymongo
from datetime import datetime
import os
import pickle
from pprint import pprint

def get_team_handles(api):
    team_file ="../data/team_handles.pkl"
    if os.path.isfile(team_file):
        team_handles= pickle.load(open(team_file, "rb"))
        print("loaded the team_handles!")
    else:
        print("need to build from scrath")
        team_handles = []
        for member in tweepy.Cursor(api.list_members, 'NFL', 'nfl-teams').items():
            team_handles.append(member.screen_name)
        pickle.dump(set(team_handles), open(team_file, "wb"))
    return team_handles
def get_team_maintained_lists(api):
    #The output of this is being saved in
    team_handles=get_team_handles(api)
    slug_file ="../data/team_maint_player_lists.pkl"
    if os.path.isfile(slug_file):
        slugs= pickle.load(open(slug_file, "rb"))
        print("loaded the team slugs!")
    else:
        slugs ={}
    for team in team_handles:
        if team not in slugs.keys():
            results = api.lists_all(team)
            #print("***Team***"+team)
            s_list=[]
            for l in results:
                s_list.append(l)
                print(l.slug + " : " + l.description)
                #time.sleep(90)
            #slugs[team]=s_list
    return slugs


def get_team_roster_from_slug(api,team):
    #The output of this is being saved in
    #team_handles=get_team_handles(api)
    slugs =get_team_maintained_lists(api)


    roster_file = "../data/rosters/"+team+"_roster.p"
    if not os.path.isfile(roster_file):
        roster = []
        for slug in slugs[team]:
            print(team+":"+slug)
            for member in tweepy.Cursor(api.list_members, team,slug).items():
                    roster.append(member.screen_name)
        print("Dumping "+team)
        pickle.dump({"team":set(roster)}, open(roster_file, "wb"))
    else:
        print("skipping "+team+" seems to be complete")



def get_player_handles(api):
    player_file ="../data/player_handles.pkl"
    if os.path.isfile(player_file):
        player_handles= pickle.load(open(player_file, "rb"))
        print("loaded the team_handles!")
    else:
        print("building player handles")
        player_handles = []
        for member in tweepy.Cursor(api.list_members, 'feve10', 'NFL-Players').items():
            player_handles.append(member.screen_name)
        pickle.dump(set(player_handles), open(player_file, "wb"))
    return player_handles

def build_team_collection(db,api):
    team_collection = db.Teams
    #team_handle = "AtlantaFalcons"
    team_handles = get_team_handles(api)
    for team_handle in team_handles:
        if team_collection.find({'_id': team_handle}).count() == 0:
            print("trying to extract team")
            team = create_team_dict(team_handle)
            team_collection.insert_one(team)


def build_player_collection(db,api):
    player_collection = db.Players
    teams = db.Teams
    player_handles=get_player_handles(api)
    for player_handle in player_handles:
        if player_collection.find({"_id":player_handle}).count() == 0:
            player = creat_player_dict(player_handle,teams)
            player_collection.insert_one(player)



def creat_player_dict(player_handle,teams):
    player ={}
    player["_id"] = player_handle
    affiliations =[]
    for team in teams.find({}):
        if player_handle in team["known_players"]:
            affiliations.append(team["_id"])

    player["affiliations"] = affiliations
    return player

def extract_player_set_from_pickle(team_handle):
    roster_file = "../data/rosters/" + team_handle + "_roster.p"
    players = pickle.load(open(roster_file, "rb"))
    print(players)
    return(players)

def create_team_dict(team_handle):
    team ={}
    team["_id"] =team_handle
    team["known_players"] = list(extract_player_set_from_pickle(team_handle)["team"])
    return team




def main():
    roster_file = "../data/falcons_handles.csv"
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
    build_team_collection(db,api)
    build_player_collection(db, api)
    # Make/Connect to a collection of tweets

# This website would probably be super helpful if we go deeper. Would need to build a scraper
#https://www.footballdb.com/teams/nfl/atlanta-falcons/alltime-roster






main()

    #This is saved in data/team_maint_player_lists.pkl
    # list_slugs = {'49ers': ['players', 'alumni'],
    #                 'AZCardinals': ['arizona-cardinals-players'],
    #              'AtlantaFalcons': ['Players', 'rookies-in-2018', 'falcons-alumni'],
    #              'Bengals': ['players'],
    #              'Broncos': ['broncos-players1'],
    #              'Browns': ['browns-players'],
    #              'Buccaneers': [],
    #              'Chargers': ['team-roster', 'alum'],
    #              'ChicagoBears': ['bears-players'],
    #              'Chiefs': ['chiefs-alumni', 'chiefs-players'],
    #              'Colts': ['colts-players'],
    #              'Eagles': ['eagles-alumni', 'philadelphia-eagles'],
    #              'Giants': ['giants-players'],
    #              'HoustonTexans': ['houston-texans-players'],
    #              'Jaguars': ['players'],
    #              'Lions': ['players', 'alumni'],
    #              'MiamiDolphins': ['dolphins'],
    #              'Panthers': ['panthers-players'],
    #              'Patriots': ['Patriots-Players'],
    #              'RAIDERS': ['raiders-roster'],
    #              'RamsNFL': ['los-angeles-rams-roster', 'rams-legends'],
    #              'Ravens': ['former-ravens', 'current-players'],
    #              'Redskins': ['redskins-alumni1'],
    #              'Saints': ['saints-players'],
    #              'Seahawks': ['players1'],
    #              'Titans': ['players'],
    #              'Vikings': ['players'],
    #              'buffalobills': ['alumni', 'players'],
    #              'dallascowboys': ['players1'],
    #              'nyjets': ['players'],
    #              'packers': [],
    #              'steelers': ['former-steelers-players', 'players']}

    #this is saved in "../data/team_handles.pkl"
    # team_handles = [u'AZCardinals',
    #                  u'Giants',
    #                  u'Colts',
    #                  u'Eagles',
    #                  u'Jaguars',
    #                  u'Panthers',
    #                  u'ChicagoBears',
    #                  u'Lions',
    #                  u'49ers',
    #                  u'Browns',
    #                  u'Redskins',
    #                  u'Buccaneers',
    #                  u'packers',
    #                  u'Chiefs',
    #                  u'Saints',
    #                  u'Patriots',
    #                  u'Vikings',
    #                  u'buffalobills',
    #                  u'Bengals',
    #                  u'RamsNFL',
    #                  u'Seahawks',
    #                  u'Ravens',
    #                  u'MiamiDolphins',
    #                  u'steelers',
    #                  u'Titans',
    #                  u'Broncos',
    #                  u'HoustonTexans',
    #                  u'nyjets',
    #                  u'AtlantaFalcons',
    #                  u'RAIDERS',
    #                  u'dallascowboys',
    #                  u'Chargers',
    #                  u'AZCardinals',
    #                  u'Giants',
    #                  u'Colts',
    #                  u'Eagles',
    #                  u'Jaguars',
    #                  u'Panthers',
    #                  u'ChicagoBears',
    #                  u'Lions',
    #                  u'49ers',
    #                  u'Browns',
    #                  u'Redskins',
    #                  u'Buccaneers',
    #                  u'packers',
    #                  u'Chiefs',
    #                  u'Saints',
    #                  u'Patriots',
    #                  u'Vikings',
    #                  u'buffalobills',
    #                  u'Bengals',
    #                  u'RamsNFL',
    #                  u'Seahawks',
    #                  u'Ravens',
    #                  u'MiamiDolphins',
    #                  u'steelers',
    #                  u'Titans',
    #                  u'Broncos',
    #                  u'HoustonTexans',
    #                  u'nyjets',
    #                  u'AtlantaFalcons',
    #                  u'RAIDERS',
    #                  u'dallascowboys',
    #                  u'Chargers']
