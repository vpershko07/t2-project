import os
import sqlite3

import pandas as pandas

from consts import CSV_DIR, DB_PATH


def remove_database():
    print("Removing database")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


def load_database():
    # open database connection to db.sqlite3
    con = sqlite3.connect(DB_PATH)

    # loop over the csv file and insert the data into the database
    for file_name in os.listdir(CSV_DIR):
        print("Loading %s" % file_name)
        table_name = file_name.split(".")[0]
        full_path = os.path.join(CSV_DIR, file_name)
        df = pandas.read_csv(full_path)
        df.to_sql(table_name, con, if_exists="replace", index=False)

    # commit the changes
    con.commit()
    # close the connection
    con.close()
    print("Database loaded")


if __name__ == "__main__":
    remove_database()
    load_database()
