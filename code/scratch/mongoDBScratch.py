import pymongo



# loop through search and insert dictionary into mongoDB
#for tweet in search:
#    # Empty dictionary for storing tweet related data
#    data ={}
#    data['created_at'] = tweet.created_at
##    data['from_user'] = tweet.from_user
#    data['from_user_id'] = tweet.from_user_id
#    data['from_user_id_str'] = tweet.from_user_id_str
#    data['from_user_name'] = tweet.from_user_name
#    data['geo'] = tweet.geo
#    data['id'] = tweet.id
#    data['iso_language_code'] = tweet.iso_language_code
#    data['source'] = tweet.source
#    data['text'] = tweet.text
#    data['to_user'] = tweet.to_user
#    data['to_user_id'] = tweet.to_user_id
#    data['to_user_id_str'] = tweet.to_user_id_str
#    data['to_user_name'] = tweet.to_user_name
#    # Insert process
#    posts.insert(data)


#Some useful tutorials
#http://altons.github.io/python/2013/01/21/gentle-introduction-to-mongodb-using-pymongo/

#https://realpython.com/introduction-to-mongodb-and-python/
# Connection to Mongo DB
try:
    conn=pymongo.MongoClient()
    print "Connected successfully!!!"
except pymongo.errors.ConnectionFailure, e:
   print "Could not connect to MongoDB: %s" % e

#To see what DB are available
print conn.database_names()

#To delete a DB completley
#conn.drop_database('<DBNAME>')


#Connect to the NFL Players Tweet DataBase
db = conn.NFLPlayerTweets
#Make/Connect to a collection of tweets
tweets = db.Tweets

#If you need to get some statistics about your databases.

#db.command({'dbstats': 1})

#To get collection statistics use the collstats command:


#db.command({'collstats': 'posts'})

