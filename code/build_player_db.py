import tweepy
import pymongo
import os
import pickle
import pandas as pd
from nameparser import HumanName
from build_team_db import get_team_mascots
# def get_team_maintained_lists(api):
#     #The output of this is being saved in
#     team_handles=get_team_handles(api)
#     slug_file ="../data/team_maint_player_lists.pkl"
#     if os.path.isfile(slug_file):
#         slugs= pickle.load(open(slug_file, "rb"))
#         print("loaded the team slugs!")
#     else:
#         slugs ={}
#     for team in team_handles:
#         if team not in slugs.keys():
#             results = api.lists_all(team)
#             #print("***Team***"+team)
#             s_list=[]
#             for l in results:
#                 s_list.append(l)
#                 print(l.slug + " : " + l.description)
#                 #time.sleep(90)
#             #slugs[team]=s_list
#     return slugs



def get_player_info_from_twitter_list(api):
    player_file ="../data/player_info.pkl"
    if os.path.isfile(player_file):
        player_info= pickle.load(open(player_file, "rb"))
    else:
        print("building player handles")
        player_info = []
        for member in tweepy.Cursor(api.list_members, 'feve10', 'NFL-Players').items():
            player_info.append(member)
        pickle.dump(set(player_info), open(player_file, "wb"))
    return player_info



def update_player_name(player_collection,handle,name):
    new_name = {"name": name}
    player_collection.update_one({"handle": handle}, {"$set": new_name})
    print(player_collection.find_one({"handle":handle}))

def build_player_collection(db,api):
    player_collection = db.Players
    player_info=get_all_player_info(api)
    for player in player_info:
        if player_collection.find({"_id":player._json["id_str"]}).count() == 0:
            player_dict = creat_player_dict(player._json)
            print(player_dict)
            player_collection.insert_one(player_dict)
            print("inserted",player_dict)



def add_player_by_username(db,api,handle):
    player_collection = db.Players
    if player_collection.find({"handle":handle}).count() == 0:
        player = api.get_user(handle)
        player_dict = creat_player_dict(player._json)
        print(player_dict)
        player_collection.insert_one(player_dict)
        print("inserted",player_dict)
    else:
        print("User already in DB")


def find_player_by_name(player_collection,first_name,last_name):
    suffix_reg_ex = '(,?\ (?:[JS]r\.?|III?|IV))?$'
    player_found = False
    player_info = first_name.capitalize() + " " + last_name.capitalize()
    player = player_collection.find({"name":{"$regex":r'^'+first_name+'.*'+last_name+suffix_reg_ex,"$options":"i"}})
    if player.count() ==0:
        #print("Player not found in DB: "+first_name+" "+ last_name)

        players = player_collection.find({"name": {"$regex": r'^' +  '.*' + last_name + suffix_reg_ex, "$options": "i"}})
        #player_info = first_name.capitalize() + " " + last_name.capitalize()
        # if players.count() >0:
        #     #print("##########Possibilities....#########")
        #     #for p in players:
        #     #    print(p)
        #     #    print("update_player_name(player_collection,'"+p['handle']+"','"+player_info+"')")
        #     #print("####################################")
        #
        # else:
        #     print("No matches by last name either for ..." +player_info )
    elif player.count() ==1:
        player_found = True
        player_info = player_collection.find_one({"name":{"$regex":r'^'+first_name+'.*'+last_name+suffix_reg_ex,"$options":"i"}})
    elif player.count()>1:

        print("MULTIPLE PLAYERS FOUND FOR "+player_info+"****************************************")
        for x in player:
            print(x)
    return player_found,player_info



# def map_players_to_handles(player_collection):
#     roster_dir = "../data/rosters/"
#
#     #for team in ["arizona-cardinals","atlanta-falcons","baltimore-ravens","buffalo-bills", "carolina-panthers", "chicago-bears", "cincinnati-bengals", "cleveland-browns", "dallas-cowboys", "denver-broncos", "detroit-lions", "green-bay-packers", "houston-texans", "indianapolis-colts", "jacksonville-jaguars", "kansas-city-chiefs", "los-angeles-chargers", "los-angeles-rams", "miami-dolphins", "minnesota-vikings", "new-england-patriots", "new-orleans-saints", "new-york-giants", "new-york-jets", "oakland-raiders", "philadelphia-eagles", "pittsburgh-steelers", "san-francisco-49ers", "seattle-seahawks", "tampa-bay-buccaneers", "tennessee-titans", "washington-redskins"]:
#     for team in [ "atlanta-falcons"]:
#         #for year in xrange(2014, 2018):
#         for year in [2017]:
#             roster = pd.read_csv(roster_dir+str(team)+"_"+str(year)+".csv")
#
#             for index, row in roster.iterrows():
#                 print(row)
#                 player_name = row['player']
#                 fname= player_name.split()[0]
#                 lname=player_name.split()[1]
#                 player_found, player_info=find_player_by_name(player_collection, fname.lower(), lname.lower())






def creat_player_dict(pl_info,):
    #Should be sending in player_info._json
    player = {}
    player["_id"] = pl_info['id_str']
    player["name"] = pl_info["name"]
    player["handle"] =pl_info["screen_name"]
    player["private"] =pl_info["protected"]

    return player


def add_team_to_player(player_collection,handle,mascot,year):
    player = player_collection.find_one({"handle": handle, "Teams": {"$exists": True}})
    new_team = [mascot, year]
    update = False
    if player == None:
        print("Table did not exist")
        d = {"Teams": [new_team]}
        update = True
    else:
        Teams = player['Teams']
        if new_team not in Teams:
            Teams.append(new_team)
            print(Teams)
            d = {"Teams": Teams}
            update = True
    if update:
        player_collection.update_one({"handle": handle}, {"$set": d})









def update_player_record(player_collection,roster_dict,player_info):
    #print(roster_dict)
    #print(player_info)
    #print("*********")
    player_collection.update_one({"handle": player_info['handle']}, {"$set": roster_dict})


    #print(player_collection.find_one({"handle":player_info['handle']}))
    print("updated ",player_info['name'])
    #1/0

def manual_update(player_collection,handle,pos,bday,college):
    roster_dict = {"pos": pos, "birthday": bday, "college": college}
    player_collection.update_one({"handle": handle}, {"$set": roster_dict})


def merge_roster_info_w_player_collection(player_collection):
    roster_dir = "../data/rosters/"
    missing_players = set([])
    team_mascots = get_team_mascots()
    #for team in ["arizona-cardinals","atlanta-falcons","baltimore-ravens","buffalo-bills", "carolina-panthers", "chicago-bears", "cincinnati-bengals", "cleveland-browns", "dallas-cowboys", "denver-broncos", "detroit-lions", "green-bay-packers", "houston-texans", "indianapolis-colts", "jacksonville-jaguars", "kansas-city-chiefs", "los-angeles-chargers", "los-angeles-rams", "miami-dolphins", "minnesota-vikings", "new-england-patriots", "new-orleans-saints", "new-york-giants", "new-york-jets", "oakland-raiders", "philadelphia-eagles", "pittsburgh-steelers", "san-francisco-49ers", "seattle-seahawks", "tampa-bay-buccaneers", "tennessee-titans", "washington-redskins"]:
    for team in ["atlanta-falcons","denver-broncos","new-england-patriots"]:
        print("STARTING "+team+"***********************")
        mascot = ""
        for t in team_mascots:
            if t.lower() in team:
                mascot = t
                print("Using "+mascot+" for the mascot")
        if mascot == "":
            print("unable to asign mascot for "+ team)
            1/0

        for year in xrange(2014, 2018):
            roster = pd.read_csv(roster_dir+str(team)+"_"+str(year)+".csv")

            for index, row in roster.iterrows():
                #print(row)
                player_name = row['player']
                #fname= player_name.split()[0]
                #lname=player_name.split()[1]
                name = HumanName(player_name)
                fname = name.first
                if name.title in ["Duke","Prince","Marquis"]:
                    fname =name.title
                lname= name.last
                #print("Searching for...",player_name)
                player_found, player_info=find_player_by_name(player_collection, fname.lower(), lname.lower())
                if player_found:
                    d_row =row.to_dict()
                    del d_row["player"]
                    #print(team,year)
                    update_player_record(player_collection, d_row, player_info)
                    add_team_to_player(player_collection=player_collection, handle=player_info["handle"],mascot=mascot,year= str(year))
                else:
                    missing_players.add(player_info)
        print(len(missing_players),missing_players)


def main():
    keyFile = open('api_key.keys', 'r')
    consumer_key = keyFile.readline().strip()
    consumer_secret = keyFile.readline().strip()
    OAUTH_TOKEN = keyFile.readline().strip()
    OAUTH_TOKEN_SECRET = keyFile.readline().strip()
    keyFile.close()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    api = tweepy.API(auth)


    try:
        db_conn = pymongo.MongoClient()
        print "DB Connected successfully!!!"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e

    db = db_conn.NFL
    build_player_collection(db, api)
    # Make/Connect to a collection of tweets

# This website would probably be super helpful if we go deeper. Would need to build a scraper
#https://www.footballdb.com/teams/nfl/atlanta-falcons/alltime-roster






#main()

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
