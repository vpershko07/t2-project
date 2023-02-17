from consts import DB_PATH
from db import get_db_connection
from repository import T2Repository
import pandas

repository = T2Repository(get_db_connection(DB_PATH))

def main():
    query = repository.get_verified_profiles_in_waitlist()
    pandas.DataFrame(query).to_csv('verified_profiles_in_waitlist.csv', index=False)

if __name__ == "__main__":
    main()
