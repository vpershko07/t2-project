import click

from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository

repository = T2Repository(get_db_connection(DB_PATH))


@click.command(help="Find the next most connected users on the waitlist")
@click.option("--limit", default=200, help="Limit the number of users to return")
def main(limit):
    best_candidates = repository.find_next_most_connected_users_on_wait_list_updated(limit)
    # print("The next most connected users on the waitlist are:")
    for candidate in best_candidates:
        list_of_targets = [f"https://t2.social/{target}" for target in candidate['target_handles'].split(',')]
        print(f"candidate: {candidate['user_twitter_handle']}, conneceted to: {list_of_targets}")


if __name__ == "__main__":
    main()
