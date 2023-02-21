from consts import (
    CONNECTIVITY_TABLE,
    OLD_WAITLIST_TABLE,
    T2_CURRENT_USERS_TABLE,
    TWITTER_USERS_TABLE,
    NEW_WAITLIST_TABLE,
    ALREADY_CRAWLED_TABLE,
    NEW_WAITLIST_CONNECTION_TABLE,
    TWITTER_VERIFIED_TABLE
)

from datetime import datetime


class T2Repository:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.cursor = db_connection.cursor()

    def find_next_most_connected_users_on_wait_list(self, limit=200):
        self.cursor.execute(
            f"SELECT user_twitter_handle, target_handle, count(*) as following FROM `{CONNECTIVITY_TABLE}` "
            f"join {T2_CURRENT_USERS_TABLE} on target_handle = twitter_username group by user_twitter_handle"
            f"order by count(*) desc limit {limit}"
        )
        return self.cursor.fetchall()


    def find_next_most_connected_users_on_wait_list_updated(self, limit=200):        
        self.cursor.execute(f"SELECT user_twitter_handle, GROUP_CONCAT(target_handle) as target_handles, count(*) as following FROM {CONNECTIVITY_TABLE} JOIN {T2_CURRENT_USERS_TABLE} on target_handle = twitter_username WHERE  user_twitter_handle not in (SELECT twitter_username FROM {T2_CURRENT_USERS_TABLE}) GROUP BY user_twitter_handle order by count(*) desc limit {limit}")
        return self.cursor.fetchall()

    def find_next_most_connected_users_on_wait_list_updated_v2(self, limit=200):
        self.cursor.execute(f"SELECT user_twitter_handle,GROUP_CONCAT(target_handle) as target_handles,count(*) as following, GROUP_CONCAT({T2_CURRENT_USERS_TABLE}.handle) as T2_handle FROM {CONNECTIVITY_TABLE} JOIN {T2_CURRENT_USERS_TABLE} on `target_twitter_info.twitter_id` = pid WHERE user_twitter_handle NOT IN (SELECT ph FROM {T2_CURRENT_USERS_TABLE} WHERE ph IS NOT NULL) GROUP BY user_twitter_handle order by count(*) desc limit {limit}")
        return self.cursor.fetchall()

    def find_recommondation_new_waitlist_connection(self, twitterhandle):
        self.cursor.execute(f"SELECT GROUP_CONCAT(DISTINCT t2_username) as t2_usernames FROM {T2_CURRENT_USERS_TABLE} JOIN {NEW_WAITLIST_CONNECTION_TABLE} on twitter_username = user_handle AND followed_by_handle=?", (twitterhandle,))
        return self.cursor.fetchall()

    def find_recommondation_connection_list(self, twitterhandle):
        self.cursor.execute(f"SELECT GROUP_CONCAT(DISTINCT t2_username) as t2_usernames FROM {T2_CURRENT_USERS_TABLE} JOIN {CONNECTIVITY_TABLE} on twitter_username = target_handle AND user_twitter_handle=?", (twitterhandle,))
        return self.cursor.fetchall()

    def get_verified_profiles_in_waitlist(self):
        self.cursor.execute(f"SELECT DISTINCT * from {TWITTER_USERS_TABLE} WHERE (twitter_id IN (SELECT  id FROM {TWITTER_VERIFIED_TABLE}) AND username IN (SELECT `What is your handle on Twitter?` FROM {NEW_WAITLIST_TABLE}))")
        return self.cursor.fetchall()    
    
    def get_t2_user_by_twitter_username(self, twitter):
        self.cursor.execute(f"SELECT * FROM {T2_CURRENT_USERS_TABLE} WHERE twitter_username = ?", (twitter,))
        return self.cursor.fetchone()
    
    def get_name_by_twitter_username(self, twitter):
        self.cursor.execute(f"SELECT name FROM {TWITTER_USERS_TABLE} WHERE username = ?", (twitter,))
        return self.cursor.fetchone()

    def get_twitter_info_by_twitter_username(self, twitter):
        self.cursor.execute(f"SELECT `public_metrics.tweet_count`,`public_metrics.following_count`, `public_metrics.followers_count` FROM {TWITTER_USERS_TABLE} WHERE username = ?", (twitter,))
        return self.cursor.fetchone()

    def get_twitter_info_by_twitter_id(self, twitter_id):
        self.cursor.execute(f"SELECT `public_metrics.tweet_count`,`public_metrics.following_count`, `public_metrics.followers_count` FROM {TWITTER_USERS_TABLE} WHERE twitter_id = ?", (twitter_id,))
        return self.cursor.fetchone()
    
    def is_user_profile_exist_by_id(self, twitter_id):
        self.cursor.execute(f"SELECT count(*) as existing FROM {TWITTER_USERS_TABLE} WHERE twitter_id=?",(twitter_id,))
        return self.cursor.fetchone()['existing']    

    def get_email_from_t2handle(self, t2handle):
        """getting email address from waiting list using t2 handle information"""
        self.cursor.execute(f"SELECT `Email Address` FROM {NEW_WAITLIST_TABLE} WHERE `#1 choice` == ? OR `#2 choice` == ? OR `#3 choice`==?",
                            (t2handle, t2handle, t2handle),
        )
        return self.cursor.fetchone()

    
    def get_twitter_user_by_username(self, twitter):
        self.cursor.execute(f"SELECT * FROM {TWITTER_USERS_TABLE} WHERE username = ?", (twitter,))
        return self.cursor.fetchone()

    def find_waiter_by_twitter_handle(self, twitter):
        self.cursor.execute(f'SELECT * FROM new_waitlist WHERE "What is your handle on Twitter?" = ?', 
                            (twitter,))
        return self.cursor.fetchone()

    def get_all_waiters_twitter_handle(self):
        self.cursor.execute(f'SELECT "What is your handle on Twitter?" FROM {NEW_WAITLIST_TABLE}')
        return self.cursor.fetchall()
    
    def is_twitter_handle_crawled(self, twitter_handle):
        self.cursor.execute(f"SELECT count(*) as existing FROM {ALREADY_CRAWLED_TABLE} WHERE user_handle = ?",(twitter_handle, ))
        return self.cursor.fetchone()['existing']


    def find_following_on_wait_list(self, twitter):
        self.cursor.execute(
            f"SELECT * FROM {OLD_WAITLIST_TABLE} join {CONNECTIVITY_TABLE}"
            f" on {OLD_WAITLIST_TABLE}.username = {CONNECTIVITY_TABLE}.target_handle"
            f" WHERE {CONNECTIVITY_TABLE}.user_twitter_handle = ?",
            (twitter,),
        )
        return self.cursor.fetchall()

    def find_followers_on_wait_list(self, twitter):
        self.cursor.execute(
            f"SELECT * FROM {OLD_WAITLIST_TABLE} join {CONNECTIVITY_TABLE} on {OLD_WAITLIST_TABLE}.username = {CONNECTIVITY_TABLE}.user_twitter_handle WHERE {CONNECTIVITY_TABLE}.target_handle = ?",
            (twitter,),
        )
        return self.cursor.fetchall()

    def find_t2_following(self, twitter):
        self.cursor.execute(
            f"SELECT * FROM {T2_CURRENT_USERS_TABLE} join {CONNECTIVITY_TABLE} on {T2_CURRENT_USERS_TABLE}.twitter_username = {CONNECTIVITY_TABLE}.target_handle WHERE {CONNECTIVITY_TABLE}.user_twitter_handle = ?",
            (twitter,),
        )
        return self.cursor.fetchall()

    def find_t2_followers(self, twitter):
        self.cursor.execute(
            f"SELECT * FROM {T2_CURRENT_USERS_TABLE} join {CONNECTIVITY_TABLE} on {T2_CURRENT_USERS_TABLE}.twitter_username = {CONNECTIVITY_TABLE}.user_twitter_handle WHERE {CONNECTIVITY_TABLE}.target_handle = ?",
                (twitter,),
            )
        return self.cursor.fetchall()

    def is_user_profile_exist(self, twitter_handle):
        self.cursor.execute(f"SELECT count(*) as existing FROM {TWITTER_USERS_TABLE} WHERE username=?",(twitter_handle,))
        return self.cursor.fetchone()['existing']

    def get_user_profile_id(self,twitter_handle):
        self.cursor.execute(f"SELECT twitter_id FROM {TWITTER_USERS_TABLE} WHERE username=?",(twitter_handle,))
        try :
            return self.cursor.fetchone()['twitter_id']
        except Exception as e:
            return None

    def insert_user_profile(self, twitter_user_json_obj):
        """inserting new profile in twitter profiles table"""
        self.cursor.execute(f"INSERT INTO {TWITTER_USERS_TABLE}(__path__, __id__, account_protected,created_at,description, location , name, profile_image_url, 'public_metrics.followers_count' , 'public_metrics.following_count','public_metrics.listed_count','public_metrics.tweet_count',twitter_created_at, twitter_id, url, username, verified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (f"twitter_profiles/{twitter_user_json_obj['id_str']}",
                             twitter_user_json_obj['id_str'],
                             int(
                                twitter_user_json_obj['protected']
                                ),
                             datetime.today().isoformat(),
                             twitter_user_json_obj['description'],
                             twitter_user_json_obj['location'],
                             twitter_user_json_obj['name'],
                             twitter_user_json_obj['profile_image_url'],
                             twitter_user_json_obj['followers_count'],
                             twitter_user_json_obj['friends_count'],
                             twitter_user_json_obj['listed_count'],
                             twitter_user_json_obj['statuses_count'],
                             twitter_user_json_obj['created_at'],
                             twitter_user_json_obj['id'],
                             twitter_user_json_obj['url'],
                             twitter_user_json_obj['screen_name'],
                             int(
                                twitter_user_json_obj['verified']
                                    ),
                                        ),
                        )

        self.db_connection.commit()
        

    def insert_to_already_crawled(self,user_name,user_id):
        """insert to already crawled table"""
        self.cursor.execute(f"INSERT INTO {ALREADY_CRAWLED_TABLE}(user_handle,user_id) VALUES (?,?)",
                            (user_name, user_id,),
                                )

        self.db_connection.commit()


    def insert_to_new_waitlist_connections(self,user_name,user_id,follower_name,follower_id):
        """insert to new waitlist connections table"""
        self.cursor.execute(f"INSERT INTO {NEW_WAITLIST_CONNECTION_TABLE}(user_handle,user_id,followed_by_handle,followed_by_id) VALUES (?,?,?,?)""",
                            (user_name, user_id,follower_name,follower_id),
                                )

        self.db_connection.commit()


    def get_all_twitter_users(self):
        self.cursor.execute(f"SELECT * FROM {TWITTER_USERS_TABLE} where username is not null")
        return self.cursor.fetchall()

    def get_all_connectivity(self):
        self.cursor.execute(f"SELECT * FROM {CONNECTIVITY_TABLE}")
        return self.cursor.fetchall()
