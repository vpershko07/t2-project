from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
from load_tweepy_api import get_tweepy_api
import tweepy
import time 

repository = T2Repository(get_db_connection(DB_PATH))

def crawl(twitter_handle,twitter_id,api):

    try:
        list_of_following = api.get_friends(screen_name  = twitter_handle, skip_status= True, include_user_entities= False)
        list_of_followers = api.get_followers(screen_name = twitter_handle, skip_status=True, include_user_entities=False)

        for user in list_of_following:
            repository.insert_to_new_waitlist_connections(user._json['screen_name'], user._json['id'],twitter_handle,twitter_id)

        for user in list_of_followers:
            repository.insert_to_new_waitlist_connections(twitter_handle,twitter_id,user._json['screen_name'], user._json['id'])

        repository.insert_to_already_crawled(twitter_handle,twitter_id)

    except tweepy.errors.TooManyRequests as e:
        print("[STOP] WAITING FOR 15 MINUTES")
        time.sleep(16*60)
        crawl(twitter_handle,twitter_id,api)
    
    except Exception as e:
        print(f"[ERROR] ERROR IN HANDLE {twitter_handle}; {e}")


def get_user_profile(twitter_handle, api):
    """ gets the user twitter object """
    try:
        return api.get_user(screen_name = twitter_handle,include_entities= False)
    except Exception as e :
        print(f"[ERROR] handle {twitter_handle}, becuase of {e}")
        return e 



def main():
    api = get_tweepy_api()
    waiters = repository.get_all_waiters_twitter_handle()
    
    for idx, waiter in enumerate(waiters) :
        waiter_handle = waiter['What is your handle on Twitter?'] 

        print(f"[INFO] IN HANDLE: {waiter_handle}")

        if waiter_handle is None:
            print(f"[INFO] PROBLEM WITH HANDLE:: {waiter_handle}")
            continue

        waiter_handle = waiter_handle.strip() # removing any spaces in the user-name

        if not repository.is_user_profile_exist(waiter_handle) and waiter_handle is not None:
            print("[INFO] GETTING USER PROFILE..")
            user = get_user_profile(waiter_handle, api) # getting the user object from tweepy api.
            repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.
            print("[INFO] USER INSERTED TO DATABSE.")
        else:
            print(f"[INFO] HANDLE {waiter_handle} ALREADY EXISTS IN TWIITER PROFILES TABLE")

        if (not repository.is_twitter_handle_crawled(waiter_handle)) and waiter_handle is not None:
            waiter_id = repository.get_user_profile_id(waiter_handle)
            if waiter_id is None:
                continue
            print("[INFO] CRAWLING USER REALTIONSHIPS..")
            crawl(waiter_handle,waiter_id,api)
        else:
            print(f"[INFO] HANDLE {waiter_handle} ALREADY CRAWLED")
        
        print(f"[INFO] IN USER NUMBER: {idx}")

if __name__ == "__main__":
    main()
