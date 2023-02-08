import click 
import codecs
from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
import hashlib
from load_tweepy_api import get_tweepy_api

def sha1_key(t2handle):
    return hashlib.sha1(f"{t2handle}7e##$83R73df1bFf3C9548".encode('utf-8')).hexdigest()


def rot5_encode(string):
    encoded_string = ""
    for char in string:
        if char.isdigit():
            encoded_string += chr((ord(char) - ord('0') + 5) % 10 + ord('0'))
        else:
            encoded_string += char
    return encoded_string


def get_user_profile(twitter_handle, api):
    """ gets the user twitter object """
    try:
        return api.get_user(screen_name = twitter_handle,include_entities= False)
    except Exception as e :
        print(f"[ERROR] handle {twitter_handle}, becuase of {e}")
        return e 



@click.command(help="t2 handle")
@click.option("--t2handle", help="t2 handle")
@click.option("--twitterhandle", help="twitter handle", required= False)
def main(t2handle, twitterhandle):
    repository = T2Repository(get_db_connection(DB_PATH))
    
    list_of_recommendations = []
    first_name = ""
    
    if twitterhandle is not None:
        # checking if user exists or not 
        if not repository.is_user_profile_exist(twitterhandle):
            print("[WARNING] THIS USER PROFILE DOESNOT EXIST")
            print("[INFO] CRAWLING TWITTER PROFILE")
            twitter_api  = get_tweepy_api()
            user_profile =  get_user_profile(twitterhandle, twitter_api)
            first_name = user_profile._json['name']
            id = str(user_profile._json['id'])
            repository.insert_user_profile(user_profile._json)

        else:
            id = repository.get_user_profile_id(twitter_handle=twitterhandle)
            first_name = repository.get_name_by_twitter_username(twitterhandle)['name']

        PH_SEE_BELOW  = codecs.encode(twitterhandle, 'rot_13') # rot13 encoding for twitter handle 
        PID_SEE_BELOW = rot5_encode(str(id))

        try:
            recommendation_newwatlist_connectons = repository.find_recommondation_new_waitlist_connection(twitterhandle)[0]['t2_usernames'].split(',')
        except AttributeError:
            print("[WARNING] no connections in new waitlist connections")
            recommendation_newwatlist_connectons = []
        except Exception as e:
            print(f"[WARNING] in new waitlist connections {e}")
            recommendation_newwatlist_connectons = []

        try:
            recommendation_connectivity_data = repository.find_recommondation_connection_list(twitterhandle)[0]['t2_usernames'].split(',')
        except AttributeError:
            print("[WARNING] no connections in connectivity data")
            recommendation_connectivity_data = []
        except Exception as e:
            print(f"[ERROR] in connectivity data connections {e}")
            recommendation_connectivity_data = []

        list_of_recommendations = [f"https://t2.social/{t2_handle}" for t2_handle in recommendation_newwatlist_connectons]
        list_of_recommendations.extend([f"https://t2.social/{t2_handle}" for t2_handle in recommendation_connectivity_data if t2_handle not in recommendation_newwatlist_connectons])


    KEY_SEE_BELOW = sha1_key(t2handle) # sha1 key for the t2handle
    link = f"https://t2.social/signup?handle={t2handle}&ph={PH_SEE_BELOW}&pid={PID_SEE_BELOW}&key={KEY_SEE_BELOW}" if twitterhandle is not None else f"https://t2.social/signup?handle={t2handle}&key={KEY_SEE_BELOW}"

    # check whether there is a connections or not.
    if not list_of_recommendations:
        pre_recommendations_list_text = ""
        list_of_recommendations_text = ""
    else:
        pre_recommendations_list_text = "Once you’ve signed up here are some recommendations for you for people to follow:"
        list_of_recommendations_text = '\n'.join(list_of_recommendations)


    
    message = f"""   
Subject: T2 Invite: @{t2handle}

{first_name} -
                
Really excited to welcome you to T2. Here is your invite to claim handle @{t2handle}:
{link}
{pre_recommendations_list_text}
{list_of_recommendations_text}

Thanks for joining!

Gabor
"""                
    print(message)
    return

if __name__ == '__main__':
    main()