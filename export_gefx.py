import csv
import json
import sqlite3

import click
import networkx

from consts import DB_PATH


def dict_factory(cursor, row):
    result = {}
    for index, col in enumerate(cursor.description):
        result[col[0]] = row[index]
    return result


# open database connection to db.sqlite3
con = sqlite3.connect(DB_PATH)
con.row_factory = dict_factory
cur = con.cursor()


def read_profiles():
    cur.execute("SELECT * FROM twitter_users")
    rows = cur.fetchall()
    data = {}
    for row in rows:
        data[row.get("id")] = row
    return data


def read_follow_data_json():
    f = open("twitter_following-1.json")
    data = json.load(f)

    x_follows_y = {}

    for i in data["data"]:
        o = data["data"][i]
        target_handle = o.get("target_handle")
        twitter_handle = o.get("user_twitter_handle")
        x_follows_y[twitter_handle] = target_handle

    return x_follows_y


def read_follow_data_csv():
    x_follows_y = {}
    cur.execute("SELECT * FROM connectivity")
    rows = cur.fetchall()
    for row in rows:
        x_follows_y[row.get("user_twitter_handle")] = row.get("target_handle")
    return x_follows_y


def create_digraph(data, follow_data, verified):
    G = networkx.DiGraph()

    was_verified_count = 0
    follow_count = 0

    for twitter_id, twitter_data in data.items():
        username = twitter_data.get("username")
        account_protected = twitter_data.get("protected") or ""
        description = twitter_data.get("description") or ""
        name = twitter_data.get("name") or ""
        location = twitter_data.get("location") or ""
        followers_count = twitter_data.get("followers_count")
        was_verified = twitter_id in verified
        if was_verified:
            was_verified_count = was_verified_count + 1

        G.add_node(
            username,
            twitter_id=twitter_id,
            username=username,
            account_protected=account_protected,
            description=description,
            name=name,
            location=location,
            followers_count=followers_count,
            was_verified=was_verified,
        )

    print("was_verified_count", was_verified_count)

    for i in follow_data:
        follower = i
        for following in follow_data[i]:
            if G.has_node(follower) and G.has_node(following):
                G.add_edge(follower, following)
                follow_count = follow_count + 1

    print("follow_count", follow_count)

    return G


def read_verified_data():
    verified = {}
    cur.execute("SELECT * FROM twitter_users")
    rows = cur.fetchall()
    for row in rows:
        verified[row.get("id")] = row.get("username")
    return verified


@click.command()
@click.option("--output", default="graph.gexf", help="Output file name")
def main(output):
    print("Starting ...")
    verified = read_verified_data()
    follow_data = read_follow_data_csv()
    data = read_profiles()
    G = create_digraph(data, follow_data, verified)
    networkx.write_gexf(G, "graph.gexf", encoding="utf-8", prettyprint=True)


if __name__ == "__main__":
    main()
