from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
from load_tweepy_api import get_tweepy_api
import tweepy

repository = T2Repository(get_db_connection(DB_PATH))


def process_page(page, twitter_handle, twitter_id,friend):
    """ Entering every user to the DB. """
    counter = 0
    if friend:
        for user in page:
            repository.insert_to_new_waitlist_connections(user._json['screen_name'], user._json['id'],twitter_handle,twitter_id)
            counter += 1
    else:
        for user in page:
            repository.insert_to_new_waitlist_connections(twitter_handle,twitter_id,user._json['screen_name'], user._json['id'])
            counter +=1
    
    print(f"[INFO] PAGE HAS {counter} OF USERS")


def crawl(twitter_handle,twitter_id,api):
    """ Crawling user friends/followers """
    try:
        for idx, page in enumerate(tweepy.Cursor(api.get_friends, screen_name  = twitter_handle, skip_status= True, include_user_entities= False, count=200).pages()):
            process_page(page, twitter_handle, twitter_id, friend=True)
            print(f"[INFO] IN HANDLE {twitter_handle}, IN PAGE NUMBER {idx} OF FRIENDS")

        for idx, page in enumerate(tweepy.Cursor(api.get_followers, screen_name = twitter_handle, skip_status=True, include_user_entities=False, count=200).pages()):
            process_page(page, twitter_handle, twitter_id, friend=False)
            print(f"[INFO] IN HANDLE {twitter_handle}, IN PAGE NUMBER {idx} OF FOLLOWING")

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


            if (not repository.is_twitter_handle_crawled(waiter_handle)) and waiter_handle is not None:
                waiter_id = repository.get_user_profile_id(waiter_handle)
                if waiter_id is None:
                    continue
                print("[INFO] CRAWLING USER REALTIONSHIPS..")
                crawl(waiter_handle,waiter_id,api)
            else:
                print(f"[INFO] HANDLE {waiter_handle} ALREADY CRAWLED")

            print(f"[INFO] IN USER NUMBER: {idx}")

        except Exception as e :
            print(f"[ERROR] FATAL ERROR IN CRAWL MAIN LOOP,{e}")

if __name__ == "__main__":
    main()
