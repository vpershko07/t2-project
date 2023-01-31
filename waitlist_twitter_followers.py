from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
from load_tweepy_api import get_tweepy_api
import tweepy
import click

repository = T2Repository(get_db_connection(DB_PATH))


def process_page(page, twitter_handle, twitter_id):
    """ Entering every user to the DB. """
    for user in page:
        repository.insert_to_new_waitlist_connections(twitter_handle,twitter_id,user._json['screen_name'], user._json['id'])

        t2_username = repository.get_t2_user_by_twitter_username(user._json['screen_name'])
        if t2_username:
            print(f"[INFO] {user._json['screen_name']}, https://twitter.com/{user._json['screen_name']} ,{user._json['followers_count']} followers, {user._json['friends_count']} followings,t2: {t2_username['t2_username']} ,https://t2.social/{t2_username['t2_username']}")
        else:
            print(f"[INFO] {user._json['screen_name']}, https://twitter.com/{user._json['screen_name']} ,{user._json['followers_count']} followers, {user._json['friends_count']} followings")
        
        # Insert it into twitter profiles
        if not repository.is_user_profile_exist(user._json['screen_name']) and user._json['screen_name']is not None:
            repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.



def crawl(twitter_handle,twitter_id,api):
    """ Crawling user friends/followers """
    try:
        for idx, page in enumerate(tweepy.Cursor(api.get_followers, screen_name = twitter_handle, skip_status=True, include_user_entities=False, count = 200).pages()):
            process_page(page, twitter_handle, twitter_id)
        repository.insert_to_already_crawled(twitter_handle,twitter_id)
    except Exception as e:
        print(f"[ERROR] ERROR IN HANDLE {twitter_handle}; {e}")



def get_user_profile(twitter_handle, api):
    """ gets the user twitter object """
    try:
        return api.get_user(screen_name = twitter_handle,include_entities= False)
    except Exception as e :
        print(f"[ERROR] handle {twitter_handle}, becuase of {e}")
        return e 


@click.command(help="waitlist twitter followers")
@click.option("--twitterhandle", type=str, help="twitter username")
def main(twitterhandle):
    api = get_tweepy_api()
    
    try:
        user_handle = twitterhandle.strip()
        print(f"[INFO] IN HANDLE: {user_handle}")

        if user_handle is None or user_handle == "":
            print(f"[INFO] NO TWITTER HANDLE:: {user_handle}")
            return

        # checking the presence of user's profile in twitter profiles DB.
        if not repository.is_user_profile_exist(user_handle) and user_handle is not None:
            print("[INFO] GETTING USER PROFILE..")
            user = get_user_profile(user_handle, api) # getting the user object from tweepy api.
            repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.
            print("[INFO] USER INSERTED TO DATABSE.")
        else:
            print(f"[INFO] HANDLE {user_handle} ALREADY EXISTS IN TWIITER PROFILES TABLE")


        # Getting connections info.
        print(f"[INFO] LIST OF PEOPLE FOLLOWIING {user_handle}")
        if (not repository.is_twitter_handle_crawled(user_handle)) and user_handle is not None:
            waiter_id = repository.get_user_profile_id(user_handle)
            if waiter_id is None:
                waiter_id = user._json['id']
            crawl(user_handle,waiter_id,api)
        # username's twitter followers already in DB.         
        else:
            print(f"[INFO] LIST OF PEOPLE FOLLOWIING {user_handle}")

    except Exception as e :
        print(f"[ERROR] {e}")


if __name__ == '__main__':
    main()