import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
        - read the data in each JSON song file and load it in pandas dataframe
        - select 'song_id' , 'title' , 'artist_id' , 'year' , 'duration' and insert it into the songs table in the database
        - select 'artist_id' , 'artist_name' , 'artist_location' , 'artist_latitude' , 'artist_longitude' and insert it into the artists table in the database
    """
    # open song file
    df = pd.read_json(filepath , lines=True)

    # insert song record
    song_data = df[['song_id' , 'title' , 'artist_id' , 'year' , 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id' , 'artist_name' , 'artist_location' , 'artist_latitude' , 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
        - read the data in each JSON log file and load it in pandas dataframe
        - filter by attribute page=="NextSong"
        - select 'timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday' and insert it into the time table in the database
        - select 'userId' , 'firstName' , 'lastName' , 'gender' , 'level' and insert it into the users table in the database
        - select timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent and insert it into the songplays table in the database
    """
    # open log file
    df = pd.read_json(filepath , lines=True)

    # filter by NextSong action
    df = df[df.page=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['t'] = t
    # insert time data records ******list(df.ts.values) 
    time_data = [ list(t.values) , t.dt.hour.tolist() , t.dt.day.tolist() , t.dt.week.tolist() , t.dt.month.tolist() , t.dt.year.tolist() , t.dt.weekday.tolist() ]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId' , 'firstName' , 'lastName' , 'gender' , 'level' ]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = list( (row.t, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)  )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
        - fetch the path of all log files and song files
        - count the number of the fimes found and display it
        - execute the functions above
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        - the main funtion for the etl job
        - connect to the database
        - calls the functions to process the log and song files, which insert the data into the tables
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()