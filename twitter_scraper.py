import tweepy
import mysql.connector
import credentials
from mysql.connector.constants import ClientFlag
from textblob import TextBlob
import re

class MyStreamListener(tweepy.StreamListener):
    '''
    Tweets are known as “status updates”. So the Status class in tweepy has properties describing the tweet.
    https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
    '''

    def on_status(self, status):
        '''
        Extract info from tweets
        '''

        if status.retweeted != True:
            # Avoid retweeted info, and only original tweets will be received
            # Extract attributes from each tweet
            id_str = status.id_str
            created_at = status.created_at
            text = clean_tweet(deEmojify(status.text))  # Pre-processing the text
            sentiment = TextBlob(text).sentiment
            polarity = sentiment.polarity
            subjectivity = sentiment.subjectivity

            user_created_at = status.user.created_at
            user_location = deEmojify(status.user.location)
            user_description = deEmojify(status.user.description)
            user_followers_count = status.user.followers_count
            longitude = None
            latitude = None
            if status.coordinates:
                longitude = status.coordinates['coordinates'][0]
                latitude = status.coordinates['coordinates'][1]

            retweet_count = status.retweet_count
            favorite_count = status.favorite_count


            print(clean_tweet(deEmojify(status.text)) )
            print("Long: {}, Lati: {}".format(longitude, latitude))
            print(status.retweeted)
            # Store all data in MySQL
            if cnxn.is_connected():
                cursor = cnxn.cursor()
                query = "INSERT INTO altice_tweets (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                val = (id_str, created_at, text, polarity, subjectivity, user_created_at, user_location, \
                       user_description, user_followers_count, longitude, latitude, retweet_count, favorite_count)
                cursor.execute(query, val)
                cnxn.commit()
                cursor.close()

            
    def on_error(self, status_code):
        '''
        Since Twitter API has rate limits, stop srcraping data as it exceed to the thresold.
        '''
        if status_code == 420:
            # return False to disconnect the stream
            return False


def clean_tweet(tweet):
    '''
    Use sumple regex statemnents to clean tweet text by removing links and special characters
    '''
    return ' '.join(re.sub("([^0-9A-Za-z \t]) \
                                |(\w+:\/\/\S+)", " ", tweet).split())

def deEmojify(text):
    '''
    Strip all non-ASCII characters to remove emoji characters
    '''
    if text:
        return text.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


# connect to cloud sql
config = {
    'user': 'root',
    'password': 'data',
    'host': '34.74.21.37',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': 'ssl/server-ca.pem',
    'ssl_cert': 'ssl/client-cert.pem',
    'ssl_key': 'ssl/client-key.pem',
    'database': 'twitter_db'
}
table_name = 'altice_tweets'
table_attributes = "id_str VARCHAR(255), created_at DATETIME, text VARCHAR(255), \
            polarity INT, subjectivity INT, user_created_at VARCHAR(255), user_location VARCHAR(255), \
            user_description VARCHAR(255), user_followers_count INT, longitude DOUBLE, latitude DOUBLE, \
            retweet_count INT, favorite_count INT"

# establish the connection
cnxn = mysql.connector.connect(**config)

if cnxn.is_connected():
    cursor = cnxn.cursor()
    cursor.execute("""
         SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_name = '{0}'
         """.format(table_name))
    if cursor.fetchone()[0] != 1:
        cursor.execute("CREATE TABLE {} ({})".format(table_name, table_attributes))
        cnxn.commit()
    cursor.close()


auth  = tweepy.OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

tweets_to_track = ["@AlticeUSA","@optimum", "@suddenlink", "@cheddar", "@news12", "@i24news", "@a4tise", "@AlticeB2B_USA"]


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener, tweet_mode='extended')
myStream.filter(languages=["en"], track = tweets_to_track)
# Close the MySQL connection as it finished
# However, this won't be reached as the stream listener won't stop automatically
# Press STOP button to finish the process.
cnxn.close()

