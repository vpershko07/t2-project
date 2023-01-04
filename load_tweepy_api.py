import tweepy
from tweepy_access_keys import CONSUMER_KEY ,CONSUMER_SECRET ,ACCESS_TOKEN ,ACCESS_TOKEN_SECRET 

def get_tweepy_api():
    auth = tweepy.OAuth1UserHandler(
    CONSUMER_KEY ,CONSUMER_SECRET ,ACCESS_TOKEN ,ACCESS_TOKEN_SECRET
        )

    return tweepy.API(auth, wait_on_rate_limit=True)
