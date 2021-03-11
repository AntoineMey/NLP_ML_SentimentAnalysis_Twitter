from __future__ import print_function
import json
from kafka import KafkaProducer, KafkaClient
import tweepy

##### Get your access here : http://dev.twitter.com
consumer_key = 'ENTER YOUR CONSUMER KEY HERE'
consumer_secret = 'ENTER YOUR CONSUMER SECRET HERE'
access_key = 'ENTER YOUR ACCESS TOKEN KEY HERE'
access_secret = 'ENTER YOUR ACCESS TOKEN SECRET HERE'

##### Keywords
keywords = ['Bridgerton', 
         'bridgerton', 
         'arsenelupin', 
         'Arsene Lupin', 
         'arsene lupin', 
         '#netflixbridgerton', 
         '#NetflixBridgerton']

class StreamListener(tweepy.StreamListener):

    def on_connect(self):
        print("Streaming API connected")

    def on_error(self, status_code):
        print("Kafka Producer Error" + repr(status_code))
        # Keep stream alive : 
        return True 

    def on_data(self, data):
        # Push new data to kafka queue :
        try:
            producer.send('btc_twitter_stream', data.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        # Keep stream alive : 
        return True 

    def on_timeout(self):
        # Keep stream alive : 
        return True 

# Setup Kafka : 
producer = KafkaProducer(bootstrap_servers=['localhost:5042'])

# Authentication : 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# Setup the listener :
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=60, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
stream = tweepy.Stream(auth=auth, listener=listener)
print(str(keywords))
stream.filter(track=keywords, languages = ['en'])