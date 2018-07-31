import twython as twy
import tweepy
import pymongo
import os
import pickle
from pprint import pprint
import glob
import pandas as pd
from nameparser import HumanName
from build_player_db import find_player_by_name


def get_team_mascots():
    team_file ="../data/mascots.p"
    if os.path.isfile(team_file):
        team_mascots= pickle.load(open(team_file, "rb"))
        print("loaded the team_mascots!")
    else:
        print("Mascot Pickle is missing...")

    return team_mascots


def export_roster_to_csv():
    roster_dir = "../data/rosters/"
    missing_players = set([])
    team_mascots = get_team_mascots()
    with open("combined_rosters.csv","w") as f:
        for team in ["arizona-cardinals", "atlanta-falcons", "baltimore-ravens", "buffalo-bills", "carolina-panthers",
                     "chicago-bears", "cincinnati-bengals", "cleveland-browns", "dallas-cowboys", "denver-broncos",
                     "detroit-lions", "green-bay-packers", "houston-texans", "indianapolis-colts", "jacksonville-jaguars",
                     "kansas-city-chiefs", "los-angeles-chargers", "los-angeles-rams", "miami-dolphins",
                     "minnesota-vikings", "new-england-patriots", "new-orleans-saints", "new-york-giants", "new-york-jets",
                     "oakland-raiders", "philadelphia-eagles", "pittsburgh-steelers", "san-francisco-49ers",
                     "seattle-seahawks", "tampa-bay-buccaneers", "tennessee-titans", "washington-redskins"]:
            print("STARTING " + team + "***********************")
            mascot = ""
            for t in team_mascots:
                if t.lower() in team:
                    mascot = t
                    print("Using " + mascot + " for the mascot")
            if mascot == "":
                print("unable to assign mascot for " + team)
                1 / 0

            for year in xrange(2014, 2018):
                if team == "los-angeles-chargers" and year <= 2016:
                    team = "san-diego-chargers"
                elif team == "san-diego-chargers" and year > 2016:
                    team = "los-angeles-chargers"

                # Rams City Change
                if team == "los-angeles-rams" and year <= 2015:

                    team = "st-louis-rams"
                elif team == "st-louis-rams" and year > 2015:
                    team = "los-angeles-rams"

                roster = pd.read_csv(roster_dir + str(team) + "_" + str(year) + ".csv")

                for index, row in roster.iterrows():
                    player_name = row['player']
                    name = HumanName(player_name)
                    fname = name.first
                    if name.title in ["Duke", "Prince", "Marquis"]:
                        fname = name.title
                    lname = name.last
                    f.write(str(year) + "," + mascot + "," + fname+" "+lname + "\n")

                    # print("Searching for...",player_name)
                    # player_found, player_info=find_player_by_name(player_collection, fname.lower(), lname.lower())
                    # add_player_to_roster(team_collection=team_collection,mascot=mascot,year= str


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




def add_player_to_roster(team_collection,mascot,year,player_info,player_found):
    roster_key=year + "_roster"
    team = team_collection.find_one({"_id": mascot, roster_key: {"$exists": True}})

    #<todo> need to fix this
    year + "_roster"
    if player_found:
        new_player = [player_info["name"],player_info["_id"]]
    else:
        new_player = [player_info, ""]
    update = False
    if team == None:
        print("Table did not exist")
        d = {roster_key: [new_player]}
        update = True
    else:
        roster = team[roster_key]
        #<todo> if we want to do more adds later then we need a more sophisticated
        #  check here to only check first coordinate of "new player"
        if new_player not in roster:
            roster.append(new_player)
            print(roster)
            d = {roster_key: roster}
            update = True
    if update:
        team_collection.update_one({"_id": mascot}, {"$set": d})




def add_players_to_team_collection_rosters(player_collection,team_collection):
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
            print("unable to assign mascot for "+ team)
            1/0

        for year in xrange(2014, 2018):
            roster = pd.read_csv(roster_dir+str(team)+"_"+str(year)+".csv")

            for index, row in roster.iterrows():
                player_name = row['player']
                name = HumanName(player_name)
                fname = name.first
                if name.title in ["Duke","Prince","Marquis"]:
                    fname =name.title
                lname= name.last
                #print("Searching for...",player_name)
                player_found, player_info=find_player_by_name(player_collection, fname.lower(), lname.lower())
                add_player_to_roster(team_collection=team_collection,mascot=mascot,year= str(year),player_info=player_info,player_found=player_found)














def build_team_collection(db,api):
    team_collection = db.Teams
    #team_handle = "AtlantaFalcons"
    team_handles = get_team_handles(api)
    team_mascots = get_team_mascots()
    for mascot in team_mascots:
        print("STARTING *****"+mascot+"**********")
        files = glob.glob("../data/rosters/*" + mascot.lower() + "*.csv")
        for handle in team_handles:
            if mascot.lower() in handle.lower():
                if team_collection.find({'_id': mascot}).count() == 0:
                    #rint("trying to extract team")
                    team = create_team_dict(handle,mascot,files)
                    pprint(team)
                    team_collection.insert_one(team)
                    #print(files)
                else:
                    print(mascot+" already in DB.")

        print("Finished *****" + mascot + "**********")


def get_roster_from_footballDBCSV(roster_file):
    roster = pd.read_csv(roster_file)
    print(roster_file)
    return roster['player']


def create_team_dict(team_handle,mascot,files):
    team ={}
    team["handle"] =team_handle
    team["_id"] = mascot

    #for roster_file in files:
    #    players=list(set(get_roster_from_footballDBCSV(roster_file)))
    #    #print(players)
    #    #year = roster_file[-8:-4]
    #    #combined = []
    #    #for player in players:
    #    #    combined.append((player,''))
    #
    #    #team[year+"_roster"] =combined
    return team






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

    twython_api = twy.Twython(consumer_key, consumer_secret, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    try:
        db_conn = pymongo.MongoClient()
        print "DB Connected successfully!!!"
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e

    db = db_conn.NFL
    build_team_collection(db,api)
    #build_player_collection(db, api)
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
