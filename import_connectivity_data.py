""" Importing connectivity_data table into new_waitlist_connection table"""
from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
from load_tweepy_api import get_tweepy_api

def get_user_profile(twitter_handle, api):
    """ gets the user twitter object """
    try:
        return api.get_user(screen_name = twitter_handle,include_entities= False)
    except Exception as e :
        print(f"[ERROR] handle {twitter_handle}, becuase of {e}")
        return e 

def main():
    repository = T2Repository(get_db_connection(DB_PATH))
    api = get_tweepy_api()

    
    connections_query = repository.find_connections_in_connectivity_data()
    
    for user_connections in connections_query:
        user_twitter_handle = user_connections['user_twitter_handle']
        try:
            if not repository.is_user_profile_exist(user_twitter_handle):
                user = get_user_profile(user_twitter_handle, api) # getting the user object from tweepy api.
                user_twitter_id = user._json['id']
                if not repository.is_user_profile_exist_by_id(user_twitter_id):
                    print(f"[INFO] CRAWLING PROFILE : {user_twitter_handle}")
                    repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.
            else:
                user_twitter_id = repository.get_user_profile_id(user_twitter_handle)
                
            target_handles = user_connections['target_handles'].split(',')
            target_ids = user_connections['target_ids'].split(',')

            for target_id, target_handle in zip(target_ids, target_handles):
                if not repository.check_in_new_waitlist_connections(target_handle, target_id, user_twitter_handle, user_twitter_id):
                    print(f"[INFO] INSERTING CONNECTION : {target_handle} <-- {user_twitter_handle}")
                    repository.insert_to_new_waitlist_connections(target_handle, target_id, user_twitter_handle, user_twitter_id)
        except Exception as e:
            print(f"[ERROR] IN HANDLE {user_twitter_handle}; {e}")

if __name__ == "__main__":
    main()
