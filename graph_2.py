import click
import networkx

from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository

repository = T2Repository(get_db_connection(DB_PATH))


def read_profiles_and_create_nodes(graph):
    twitter_users = repository.get_all_twitter_users()
    for row in twitter_users:
        graph.add_node(
            row["username"],
            # twitter_id=twitter_id,
            # username=username,
            # account_protected=account_protected,
            # description=description,
            # name=name,
            # location=location,
            # followers_count=followers_count,
            # was_verified=was_verified,
        )


def read_follow_data_and_link_nodes(graph):
    rows = repository.get_all_connectivity()
    for row in rows:
        follower = row.get("user_twitter_handle")
        following = row.get("target_handle")
        if graph.has_node(follower) and graph.has_node(following):
            graph.add_edge(follower, following)


@click.command()
@click.option("--output", default="graph2.gexf", help="Output file name")
def main(output):
    print("Starting ...")
    graph = networkx.DiGraph()
    read_profiles_and_create_nodes(graph)
    read_follow_data_and_link_nodes(graph)
    networkx.write_gexf(graph, output, encoding="utf-8", prettyprint=True)
    print("Done")


if __name__ == "__main__":
    main()
