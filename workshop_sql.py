import json
import oauth2 as oauth
import psycopg2
import unicodedata

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

__version__ = '1.0'
__all__ = []
__author__ = 'Axel Oehmichen - ao1011@imparial.ac.uk'

def get_connection():
    # http://initd.org/psycopg/docs/usage.html
    connection = psycopg2.connect(user="postgres", password="postgres", host="127.0.0.1")
    return connection

def get_database(db):
    # http://initd.org/psycopg/docs/usage.html
    connection = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1")
    return connection

def submit_to_Database(query, query_values, connection):
    # This configuration is mandatory for an automated commit to the database
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = connection.cursor()
    answer = curs.execute(query, query_values)
    return answer

# A tweet can contain emojis and we are only interested in the text so we normalize it en encode it in ascii for the database
def normalize_text(tweet_text):
    str_text = unicodedata.normalize('NFKD', tweet_text).encode('ascii', 'ignore').strip('"')
    normalizedText = '\'' + str_text.replace("'", r"''") + '\''
    return normalizedText

#####################################
#        main program               #
#####################################

key = "XXXXXX"
secret = "YYYYYYYYYYY"

raw_connection = get_connection()
user_ids = ["25073877", "813286", "23022687", "1339835893", "216776631", "3235334092"]

if __name__ == "__main__":

    # First we create the DataBase
    database_create = "CREATE DATABASE sqlworkshop WITH OWNER = postgres ENCODING = 'UTF8' TABLESPACE = pg_default " \
                      "LC_COLLATE = 'en_GB.UTF-8' LC_CTYPE = 'en_GB.UTF-8' CONNECTION LIMIT = -1; "
    submit_to_Database(database_create, (), raw_connection)
    raw_connection.close()

    # and Tables ...
    tweet_table_create = "CREATE TABLE IF NOT EXISTS tweets(id serial, user_id char(50) NOT NULL, text char(160) " \
                         "NOT NULL, tweet_id char(50) NOT NULL, favourite_count int NOT NULL, retweet_count int NOT NULL);"
    twitter_users_create = "CREATE TABLE IF NOT EXISTS twitter_users(id serial, user_id char(50) NOT NULL, username char(50) NOT NULL, " \
                           "screen_name char(50) NOT NULL, location char(50) NOT NULL, description char(255) NOT NULL, " \
                           "followers_count int NOT NULL, friends_count int NOT NULL, listed_count int NOT NULL);"

    database_connection = get_database("sqlworkshop")
    submit_to_Database(tweet_table_create, (), database_connection)
    submit_to_Database(twitter_users_create, (), database_connection)

    # Create your consumer with the proper key/secret.
    consumer = oauth.Consumer(key=key, secret=secret)

    # Request token URL for Twitter.
    request_trend_url = "https://api.twitter.com/1.1/trends/place.json?id=1"
    request_user_url = "https://api.twitter.com/1.1/statuses/user_timeline.json?user_id="

    # Create our client.
    client = oauth.Client(consumer)

    with open("databasedump.sql", 'a') as dump:
        for twitter_user_id in user_ids:
            request_url = request_user_url + twitter_user_id
            # The OAuth Client request works just like httplib2 for the most part.
            resp, content = client.request(request_url, "GET")
            timeline_tweets_json = json.loads(content)

            user = (str(timeline_tweets_json[0]["user"]["id_str"]),
                    normalize_text(timeline_tweets_json[0]["user"]["name"]),
                    normalize_text(timeline_tweets_json[0]["user"]["screen_name"]),
                    normalize_text(timeline_tweets_json[0]["user"]["location"]),
                    normalize_text(timeline_tweets_json[0]["user"]["description"]),
                    int(timeline_tweets_json[0]["user"]["followers_count"]),
                    int(timeline_tweets_json[0]["user"]["friends_count"]),
                    int(timeline_tweets_json[0]["user"]["listed_count"]))

            user_insert = "INSERT INTO twitter_users (user_id, username, screen_name, location, description, followers_count, friends_count, listed_count) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);\n"
            #dump.write(user_insert%user)
            submit_to_Database(user_insert, user, database_connection)

            for i in range(len(timeline_tweets_json)):
                tweet_json = timeline_tweets_json[i]
                tweet_text = normalize_text(tweet_json["text"])
                tweet = (str(tweet_json["user"]["id_str"]), tweet_text, str(tweet_json["id_str"]),
                         int(tweet_json["favorite_count"]), int(tweet_json["retweet_count"]))

                tweet_insert = "INSERT INTO tweets (user_id, text, tweet_id, favourite_count, retweet_count) VALUES (%s, %s, %s, %s, %s);\n"
                #dump.write(tweet_insert%tweet)
                submit_to_Database(tweet_insert, tweet, database_connection)

    dump.close()
    database_connection.close()

