import os
import sqlite3
import click
import pandas as pandas
from consts import CSV_DIR, DB_PATH


@click.command(help="Reload a single table to the DB")
@click.option("--csv_file_name", default="t2_current_users.csv", help="name of csv file")
def load_single_table(csv_file_name):

    con = sqlite3.connect(DB_PATH)

    table_name = csv_file_name.split(".")[0]
    print(f"Loading {table_name}")
    full_path = os.path.join(CSV_DIR, csv_file_name)
    df = pandas.read_csv(full_path)
    df.to_sql(table_name, con, if_exists="replace", index=False)

    # commit the changes
    con.commit()
    # close the connection
    con.close()
    print("Database loaded")


if __name__ == "__main__":
    load_single_table()
