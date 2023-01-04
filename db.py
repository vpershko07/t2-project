import sqlite3


def get_db_connection(db_path):
    def dict_factory(cursor, row):
        # return a dictionary with the column names as keys
        result = {}
        for index, col in enumerate(cursor.description):
            result[col[0]] = row[index]
        return result

    # open sqlite3 database connection to db_path
    connection = sqlite3.connect(db_path)
    connection.row_factory = dict_factory
    return connection
