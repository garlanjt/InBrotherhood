'''
The purpose of this script is to demonstrate how to use the twython package to connect to Twitter\'s  REST API to download tweets matching search criteria.

The script has been tested on Mac OS X 10.11.3 (El Capitan) and Python 3.5.
'''

import twython as twy
import os
import time


# Set working directory
os.chdir('/path/to/your/directory/')  # I set my parent directory to a folder containing subfolders for data, figures, and scripts

# Get OAuth credentials.  Need to copy the access token and access token secret as well.  Run this code each time you are using twitteR.
APP_KEY = 'aWZoLAPZ5aSgImEwvnKG4In1l'
APP_SECRET = 'TWXKRftQs9q5UFkJwrIOJA7muEBLL7L2HATJoZ0V8ANDMZmhBM'
OAUTH_TOKEN = '24574024-xETTs1DL50vFGJbztRlg79s4EMmzlEKIrDgx5q8bz'
OAUTH_TOKEN_SECRET = 'h38UP5vQpktpyiEOv7GBaObdR43zh1lcAvkI4O4WWwo4Y'


# Connect
connection = twy.Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

# How many tweets to return per query
size = 100  # Modify as needed, maximum is 100

# One word
nba_tweets = connection.search(q='nba', count=size)  # Also returns hashtags, and Twitter is not case sensitive.

nba_tweets2 = connection.search(q='nba', count=size)  # Search queries launched almost simultaneously return different results.

# Mentions
obama = connection.search(q='@BarackObama', count=size)

# Multiple words
nba_tweets3 = connection.search(q='nba OR nra', count=size)

nba_tweets4 = connection.search(q='nba AND nra', count=size)
len(nba_tweets4['statuses'])  # 44

nba_tweets5 = connection.search(q='nba -Cavs -Cavaliers -Warriors', count=size)  # Tweets with nba but not Cavs, Cavaliers, or Warriors.  88 at the time of the query.

# Only tweets with images.  Links in tweets are to images.
nba_images = connection.search(q='nba filter:images', count=size)

# Only tweets with links.  Links in tweets are to external sites.
nba_links = connection.search(q='nba filter:links', count=size)

nba_tweets_es = connection.search(q='nba', count=size, lang='es')  # Tweets about the NBA in Spanish

# Geocode
nba_tweets_nyc = connection.search(q='nba', count=size, geocode='40.7128,-74.0059,5mi')  # Return tweets within a 500 mile radius of New York City.

# Since date. n has to be large or the topic small for this parameter to limit the number of tweets returned.
taxes = connection.search(q='tax', count=size, since_id=<your tweet ID>)  # since_id is ID of tweet, not a date.  No functionality for since a specific date.

# Until date
taxes = connection.search(q='tax', count=size, until='2016-10-25')  # Tweets before the until date.  Note that search does not return tweets older than 7 days.

### Use the search function to build a dataset in real time
# Calculate length of pause
window_minutes = 15  # The length of Twitter's rate limit window, in minutes.
requestsPer15Window = 450
delay = (window_minutes * 60) / requestsPer15Window  # Calculates how many seconds per request per window.  This number will tell the loop how many seconds to sleep.

# Build dataset.  Modify the while loop to collect data in a way consistent with your research needs.  For example, perhaps you download tweets every minute, so time.sleep(delay) will be time.sleep(60).  Or perhaps you make a simple function that you run every minute as a cron job.
i = 0
nba_tweets = connection.search(q='nba', count=size)
nba_tweets = nba_tweets['statuses']
while i <= 10:  # Run 10 cycles.
	print('On cycle ' + str(i))
	total = len(nba_tweets) - 1  #-1 because of 0 index
	next_tweets = connection.search(q='nba', count=size, since_id=nba_tweets[total]['id'])  # Only get tweets matching the criteria since the last tweet we have
	next_tweets = next_tweets['statuses']
	nba_tweets.extend(next_tweets)
	i += 1
	time.sleep(delay)  # Pause for delay seconds.  Has the function of both not tripping the rate limits and ensuring some tweets will occur.  If your search criteria return sufficiently few tweets, the loop stops.


