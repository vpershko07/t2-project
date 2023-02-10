from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
from load_tweepy_api import get_tweepy_api

repository = T2Repository(get_db_connection(DB_PATH))

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
        try:
            waiter_handle = waiter['What is your handle on Twitter?'] 
            waiter_handle = waiter_handle.strip()
            print(f"[INFO] IN HANDLE: {waiter_handle}")

            if waiter_handle is None:
                print(f"[INFO] PROBLEM WITH HANDLE:: {waiter_handle}")
                continue

            if not repository.is_user_profile_exist(waiter_handle) and waiter_handle is not None:
                print("[INFO] GETTING USER PROFILE..")
                user = get_user_profile(waiter_handle, api) # getting the user object from tweepy api.
                repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.
                print("[INFO] USER INSERTED TO DATABSE.")
            else:
                print(f"[INFO] HANDLE {waiter_handle} ALREADY EXISTS IN TWIITER PROFILES TABLE")

            print(f"[INFO] IN USER NUMBER: {idx}")

        except Exception as e :
            print(f"[ERROR] FATAL ERROR IN CRAWL MAIN LOOP,{e}")

if __name__ == "__main__":
    main()
