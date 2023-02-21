import click
from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
import csv
from load_tweepy_api import get_tweepy_api


def write_output_to_csv(outputs, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['candidate_twitter_handle','candidate_twitter_link',
                         'count_t2_connections','twitter_followers', 
                         'twitter_followings', 'tweets','connected_to',
                           ])
        for output in outputs:
            # write each output to be a row
            writer.writerow([ output['candidate'], f"https://twitter.com/{output['candidate']}",
                              output['connections'],output['twitter_followers'],
                              output['twitter_followings'],output['tweets'],
                              *output['connected_to']])

def get_user_profile(twitter_handle, api):
    """ gets the user twitter object """
    try:
        return api.get_user(screen_name = twitter_handle,include_entities= False)
    except Exception as e :
        print(f"[ERROR] handle {twitter_handle}, becuase of {e}")
        return e 



@click.command(help="Find the next most connected users on the waitlist")
@click.option("--limit", default=200, help="Limit the number of users to return")
def main(limit):
    # Connecting to DB and twitter API.
    repository = T2Repository(get_db_connection(DB_PATH))
    api = get_tweepy_api()

    best_candidates = repository.find_next_most_connected_users_on_wait_list_updated_v2(limit)
    outputs = []
    print("The next most connected users on the waitlist are:")
    for candidate in best_candidates:
        targets = candidate['T2_handle'].split(',')
        twitter_handle = candidate['user_twitter_handle']


        # Checking for twitter info.
        if not repository.is_user_profile_exist(twitter_handle):
            user = get_user_profile(twitter_handle, api) # getting the user object from tweepy api.
            if not repository.is_user_profile_exist_by_id(user._json['id']):
                print(f"[INFO] CRAWLING {twitter_handle}")
                repository.insert_user_profile(user._json) # inserting the user into twitter_profile DB.
            twitter_info = repository.get_twitter_info_by_twitter_id(user._json['id'])
        else:
            twitter_info = repository.get_twitter_info_by_twitter_username(twitter_handle)

        print(f"Candidate: https://twitter.com/{twitter_handle}, connections: {candidate['following']}, followers: {twitter_info['public_metrics.followers_count']}, following: {twitter_info['public_metrics.following_count']}, tweets: {twitter_info['public_metrics.tweet_count']}")
        outputs.append(
             {
                 'candidate': twitter_handle,
                 'connections': candidate['following'],
                 'twitter_followers': twitter_info['public_metrics.followers_count'], 
                 'twitter_followings': twitter_info['public_metrics.following_count'],
                 'tweets': twitter_info['public_metrics.tweet_count'], 
                 'connected_to': [f"https://t2.social/{target}" for target in targets],
             }
         )        
    # Write csv file
    write_output_to_csv(outputs, "next_most_connected_output.csv")

if __name__ == "__main__":
    main()
