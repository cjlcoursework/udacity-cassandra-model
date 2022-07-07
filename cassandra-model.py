
import csv
import glob
import os
import cassandra
from typing import List
import sql_queries


# ---
# ### STEP 1 - CONNECT TO A LOCAL CASSANDRA CLUSTER AND CREATE AN `EVENT_DATA` KEYSPACE

# In[16]:


from cassandra.cluster import Cluster, Session

cluster = Cluster(['127.0.0.1'])
cassandra_session = cluster.connect()

# create and use schema
cassandra_session.execute("""
            CREATE KEYSPACE IF NOT EXISTS event_data
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")

cassandra_session.set_keyspace('event_data')

print("connected to cassandra")


# ---
# ### STEP 2 - SETUP A CLASS TO MANAGE THE POSITION-BASED VALUES ARRAY

# In[17]:


class CsvRecord:
    """
    utility class to make transform from csv to columns clearer
    """
    good_record: bool = False  # always false unless all fields were found

    def __init__(self, line : List[any]):
        """
        transform one values array into named columns    
        :param 
            line: array of 11 values
        """
        if len(line) != 11:
            print(f"bad line: {','.join(line)}")
            return
        self.artist = line[0]
        self.first_name= line[1]
        self.gender = line[2]
        self.item_in_session = int(line[3])
        self.last_name= line[4]
        self.song_length = float(line[5])
        self.level = line[6]
        self.location= line[7]
        self.session_id= int(line[8])
        self.song = line[9]
        self.user_id = int(line[10])
        self.good_record = True


def get_files(event_data: str) -> List[str]:
    """
    walk root directory and get all csv files in all subdirectories  
    
    :param 
        event_data: the root directory name to walk
    
    :return
        A list of all csv files in all subdirectories
    """

    # checking your current working directory
    print(f"Getting csv files from {os.getcwd()}")

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/' + event_data

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*.csv'))

    return file_path_list


def merge_csv_data(file_path_list: List[str]) -> str:
    """
    merge a list of csv files into a single file

    :param
        file_path_list: a list of csv path names

    :return
        the merged single fle name (hard coded to event_datafile_new.csv)
    """

    # initiating an empty list of rows that will be generated from each file
    destination_file = 'event_datafile_new.csv'
    full_data_rows_list = []

    # for every filepath in the file path list dump to a massive array
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    dumped_count = 0
    hits_count = 0

    # write the array into a new master file
    with open(destination_file, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if row[0] == '':
                dumped_count += 1
                continue
            hits_count += 1
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    print(f"dumped {dumped_count} records")
    print(f"kept {hits_count} records")

    # check the number of rows in your csv file
    with open(destination_file, 'r', encoding='utf8') as f:
        print(f"wrote {sum(1 for line in f)} lines to event_datafile_new.csv")

    return destination_file


def load_csv_data(file: str, session: Session):
    """
    read the master csv file and load into cassadra tables  
    
    :param 
        file: the master csv file
        session: a cassandra.cluster.Session    
    """

    print("loading database...")
    line_count = 0

    # preconditions
    if not os.path.exists(file):
        raise (Exception(f"file named '{file}' was not found!!!"))
    if session is None:
        raise (Exception("Cassandra session is not set!!!!"))

    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            line_count += 1

            # read line array into a class instance
            r = CsvRecord(line)

            # song play sessions
            session.execute(sql_queries.insert_songplay_sessions, (
                r.session_id, r.item_in_session, r.artist, r.song, r.song_length
            ))

            # user sessions
            session.execute(sql_queries.insert_user_sessions, (
                r.user_id, r.session_id, r.item_in_session, r.first_name, r.last_name, r.artist, r.song
            ))
            # songs by user
            session.execute(sql_queries.insert_song_subscribers, (
                r.song, r.user_id, r.first_name, r.last_name
            ))
    print(f"loaded {line_count} lines ...")
    

def database_init(session: Session):
    """
    create all tables in Cassandra 
    
    :param 
        session: a cassandra.cluster.Session    
    """

    print("initializing database...")
    from cassandra.cluster import Cluster
    try:

        drop_tables(session=session)

        # songplay sessions
        session.execute(sql_queries.create_songplay_sessions)

        # user_sessions
        session.execute(sql_queries.create_user_sessions)

        # song_subscribers
        session.execute(sql_queries.create_song_subscribers)
        return session

    except Exception as e:
        print(e)
        raise e
        

def drop_tables(session: Session):
    """
    drop all tables in session (keyspace)

    :param
        session: a cassandra.cluster.Session
    """
    print("dropping database first...")
    from cassandra.cluster import Cluster
    try:
        session.execute(sql_queries.drop_songplay_sessions)
        session.execute(sql_queries.drop_user_sessions)
        session.execute(sql_queries.drop_song_subscribers)
        return session

    except Exception as e:
        print(e)
        raise e


def query_songplays(session: Session, session_id : int, ordinal: int ):
    print(f"\n---- query songplays with sessionId={session_id}, and item={ordinal} ----")
    rows = session.execute(sql_queries.select_songplay_sessions, (session_id, ordinal))
    for row in rows:
        print(row)


def query_user_sessions(session: Session, user_id : int, session_id : int):
    print(f"\n---- query user sessions with userId={user_id} and sessionId={session_id} ----")
    rows = session.execute(sql_queries.select_user_sessions, (user_id, session_id))
    for row in rows:
        print(row)


def query_song_subscribers(session: Session, song: str):
    print(f"\n---- query all users listening to '{song} ----")
    rows = session.execute(sql_queries.select_song_subscribers, (song,))
    for row in rows:
        print(row)


def main():
    database_init(session=cassandra_session)
    csv_files = get_files(event_data="event_data")
    master_file = merge_csv_data(file_path_list=csv_files)
    load_csv_data(master_file, cassandra_session)

    rows = cassandra_session.execute("select count(*) from songplay_sessions")
    for row in rows:
        print(f"songplay_sessions has {row.count} rows")

    rows = cassandra_session.execute("select count(*) from user_sessions")
    for row in rows:
        print(f"user_sessions has {row.count} rows")

    rows = cassandra_session.execute("select count(*) from song_subscribers")
    for row in rows:
        print(f"song_subscribers has {row.count} rows")

    query_songplays(session=cassandra_session, session_id=338, ordinal=4)
    query_user_sessions(session=cassandra_session, user_id=10, session_id=182)
    query_song_subscribers(session=cassandra_session, song="All Hands Against His Own")

    # drop_tables(session=cassandra_session)
    # cassandra_session.execute("drop keyspace if exists event_data")
    cassandra_session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()