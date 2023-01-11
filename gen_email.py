import click 
import codecs
from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
import hashlib

def sha1_key(t2handle):
    return hashlib.sha1(f"{t2handle}7e##$83R73df1bFf3C9548".encode('utf-8')).hexdigest()


@click.command(help="t2 handle")
@click.option("--t2handle", help="t2 handle")
@click.option("--twitterhandle", help="twitter handle")
def main(t2handle, twitterhandle):
    repository = T2Repository(get_db_connection(DB_PATH))

    # checking if user exists or not 
    if not repository.is_user_profile_exist(twitterhandle):
        print("[ERROR] THIS USER PROFILE DOESNOT EXIST")
        return

    PH_SEE_BELOW  = codecs.encode(twitterhandle, 'rot_13') # rot13 encoding for twitter handle 
    PID_SEE_BELOW = int(repository.get_user_profile_id(twitter_handle=twitterhandle))*3
    KEY_SEE_BELOW = sha1_key(t2handle) # sha1 key for the t2handle
    first_name = repository.get_name_by_twitter_username(twitterhandle)['name']

    try:
        recommendation_newwatlist_connectons = repository.find_recommondation_new_waitlist_connection(twitterhandle)[0]['t2_usernames'].split(',')
    except Exception as e:
        print(f"[ERROR] in new waitlist connections {e}")
        recommendation_newwatlist_connectons = []

    try:
        recommendation_connectivity_data = repository.find_recommondation_connection_list(twitterhandle)[0]['t2_usernames'].split(',')
    except Exception as e:
        print(f"[ERROR] in connectivity data connections {e}")
        recommendation_connectivity_data = []

    list_of_recommendations = [f"https://t2.social/{t2_handle}" for t2_handle in recommendation_newwatlist_connectons]
    list_of_recommendations.extend([f"https://t2.social/{t2_handle}" for t2_handle in recommendation_connectivity_data if t2_handle not in recommendation_newwatlist_connectons])
    list_of_recommendations_text = '\n'.join(list_of_recommendations)

    message = f"""   
Subject: T2 Invite: @{t2handle}

{first_name} -
                
Really excited to welcome you to T2. Here is your invite to claim handle @{t2handle}:
https://t2.social/signup?handle={t2handle}&ph={PH_SEE_BELOW}&pid={PID_SEE_BELOW}&key={KEY_SEE_BELOW}
Once you've signed up here are some recommendations for you for people to follow:
{list_of_recommendations_text}

Thanks for joining!

Gabor
"""                
    print(message)
    return

if __name__ == '__main__':
    main()