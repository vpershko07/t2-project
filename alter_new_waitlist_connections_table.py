import sqlite3
from consts import DB_PATH, NEW_WAITLIST_CONNECTION_TABLE

def add_id_column():
    """
     1- Creating temp table
     2- Inserting into temp table
     3- Dropping old table
     4- Creating new table
     5- Inserting into new table
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(DB_PATH)
    table_name = NEW_WAITLIST_CONNECTION_TABLE
    # Create a cursor object to execute SQL statements
    c = conn.cursor()
    c.execute(f"CREATE TABLE {table_name}_temp (user_handle TEXT, user_id TEXT, followed_by_handle TEXT, followed_by_id TEXT);")
    # Inerting into temp table all distinct values
    c.execute(f"INSERT INTO {table_name}_temp SELECT DISTINCT user_handle, user_id, followed_by_handle, followed_by_id FROM {table_name};")
    # Drop the original table
    c.execute(f"DROP TABLE {table_name};")
    # Create a new table without the id column
    c.execute(f"CREATE TABLE {table_name} (id TEXT PRIMARY KEY, user_handle TEXT, user_id TEXT, followed_by_handle TEXT, followed_by_id TEXT);")
    c.execute(f"INSERT INTO {table_name} (id, user_handle, user_id, followed_by_handle, followed_by_id) SELECT user_id || '' || followed_by_id, user_handle, user_id, followed_by_handle, followed_by_id FROM {table_name}_temp WHERE user_id || '' || followed_by_id NOT IN (SELECT id FROM {table_name});")
    # Commit the changes to the database
    conn.commit()
    # Close the database connection
    conn.close()



if __name__ == '__main__':
    add_id_column()