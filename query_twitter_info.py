import click

from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository

repository = T2Repository(get_db_connection(DB_PATH))


def check_if_exists_in_t2(twitter):
    user = repository.get_t2_user_by_twitter_username(twitter)
    if user:
        print(f"{twitter} are on T2 already with data {user}")
    else:
        print(f"{twitter} are not on T2")
    print("")


def check_twitter_stats(twitter):
    user = repository.get_twitter_user_by_username(twitter)
    if user:
        print(f"{twitter} are on twitter_users with data {user}")
    else:
        print(f"{twitter} does not have twitter stats")
    print("")


def check_wished_handle_available(handle):
    user = repository.get_t2_user_by_twitter_username(handle)
    if user:
        print(f"{handle} is taken")
    else:
        print(f"{handle} is available")


def check_wished_handles(twitter):
    row = repository.find_waiter_by_twitter_handle(twitter)
    if row:
        wished_handles = row["#1 choice"], row["#2 choice"], row["#3 choice"]
        print(f"{twitter} wanted to be on T2 with handles {wished_handles}")
        for handle in wished_handles:
            check_wished_handle_available(handle)
    else:
        print(f"{twitter} are not on new_waitlist")
    print("")


def check_twitter_verified_status(twitter):
    user = repository.get_twitter_user_by_username(twitter)
    if user:
        print(f"{twitter} verified status is {user['verified'] == 1}")
    else:
        print(f"{twitter} does not have twitter stats")
    print("")


def count_following_on_wait_list(twitter):
    following = repository.find_following_on_wait_list(twitter)

    twitter_following = [row["username"] for row in following]

    print(f"{twitter} is following {len(twitter_following)} twitter users on waitlist")
    print(twitter_following)
    print("")


def count_followers_on_wait_list(twitter):
    followers = repository.find_followers_on_wait_list(twitter)
    twitter_followers = [row["username"] for row in followers]

    print(f"{twitter} has {len(twitter_followers)} twitter followers on waitlist")
    print(twitter_followers)
    print("")


def count_t2_following(twitter):
    following = repository.find_t2_following(twitter)

    t2_following = [row["t2_username"] for row in following]

    print(f"{twitter} is following {len(t2_following)} T2 users")
    print(t2_following)
    print("")


def count_t2_followers(twitter):
    followers = repository.find_t2_followers(twitter)
    t2_followers = [row["t2_username"] for row in followers]

    print(f"{twitter} has {len(t2_followers)} T2 followers")
    print(t2_followers)
    print("")


@click.command()
@click.option("--twitter", prompt="Twitter handle", help="Twitter handle")
def main(twitter):
    check_if_exists_in_t2(twitter)
    check_twitter_stats(twitter)
    check_wished_handles(twitter)
    check_twitter_verified_status(twitter)

    count_following_on_wait_list(twitter)
    count_followers_on_wait_list(twitter)

    count_t2_following(twitter)
    count_t2_followers(twitter)


if __name__ == "__main__":
    main()
