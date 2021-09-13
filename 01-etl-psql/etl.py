import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def insert_record(cur, insert_query, df, fields):
    record = df[fields].values[0].tolist()
    cur.execute(insert_query, record)


def insert_dataframe(cur, df, insert_query):
    for i, row in df.iterrows():
        cur.execute(insert_query, list(row))


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    insert_record(cur, song_table_insert, df, 
                ['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # insert artist record
    insert_record(cur, artist_table_insert, df,
                ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])


def expand_time_data(df, ts_field):
    df['datetime'] = pd.to_datetime(df[ts_field], unit='ms')
    t = df
    t['year'] = t['datetime'].dt.year
    t['month'] = t['datetime'].dt.month
    t['day'] = t['datetime'].dt.day
    t['hour'] = t['datetime'].dt.hour
    t['weekday_name'] = t['datetime'].dt.isocalendar().week
    t['week'] = t['datetime'].dt.week

    return t


def process_log_file(cur, filepath):
    # open log file
    df = df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = expand_time_data(df, 'ts')
    
    # insert time data records
    # time_data = 
    # column_labels = 
    time_df = t[['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday_name']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    insert_dataframe(cur, time_df, time_table_insert)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    insert_dataframe(cur, user_df, user_table_insert)

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.itemInSession, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=student port=5432")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()