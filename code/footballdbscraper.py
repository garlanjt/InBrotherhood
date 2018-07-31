import urllib
from bs4 import BeautifulSoup
import pandas as pd

def get_rosters():
    export_dir = "../data/rosters/"

    for team in ["arizona-cardinals","atlanta-falcons","baltimore-ravens","buffalo-bills", "carolina-panthers", "chicago-bears", "cincinnati-bengals", "cleveland-browns", "dallas-cowboys", "denver-broncos", "detroit-lions", "green-bay-packers", "houston-texans", "indianapolis-colts", "jacksonville-jaguars", "kansas-city-chiefs", "los-angeles-chargers", "los-angeles-rams", "miami-dolphins", "minnesota-vikings", "new-england-patriots", "new-orleans-saints", "new-york-giants", "new-york-jets", "oakland-raiders", "philadelphia-eagles", "pittsburgh-steelers", "san-francisco-49ers", "seattle-seahawks", "tampa-bay-buccaneers", "tennessee-titans", "washington-redskins"]:
        for year in xrange(2014, 2018):
            team = team.strip()

            #Chargers City Change
            if team == "los-angeles-chargers" and year<=2016:

                team = "san-diego-chargers"
            elif team == "san-diego-chargers" and year>2016:
                team = "los-angeles-chargers"


            #Rams City Change
            if team == "los-angeles-rams" and year<=2015:

                team = "st-louis-rams"
            elif team == "st-louis-rams" and year>2015:
                team = "los-angeles-rams"

            url ="https://www.footballdb.com/teams/nfl/"+str(team)+"/roster/"+str(year)



            page = urllib.urlopen(url)

            #page = urllib2.urlopen(url).read()
            soup = BeautifulSoup(page)
            print(url)
            print(team+"****" + str(year))

            table = soup.find_all('table')[0]  # Grab the first table
            d = {"player":[],"pos":[],"birthday":[],"college":[]}
            for row in table.find_all("tr")[1:]:
                row_sripped= [td for td in row.stripped_strings]
                if len(row_sripped)==7:
                    _, player, pos, _, _, birthday, college =row_sripped

                    d["college"].append(college)
                else:
                    _, player, pos, _, _, birthday =row_sripped
                    d["college"].append("")
                d["player"].append(player)
                d["pos"].append(pos)
                d["birthday"].append(birthday)


                #print("#####WARNING INVALID ROW on "+str(year)+str(team))
                    #print(row_sripped)
                #print(player, pos, birthday, college)
            df = pd.DataFrame(d,columns=["player","pos","birthday","college"])
            df.to_csv(export_dir+str(team)+"_"+str(year)+".csv",index=False)
            print("*****")



get_rosters()