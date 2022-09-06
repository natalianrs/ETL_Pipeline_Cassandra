# Importing Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
from cql_statements import *

def pre_processing_data():
    # Creating list of filepaths to process data files
    print(('Processing data files from'), os.getcwd())
    filepath = os.getcwd() + '/event_data'
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))

    # Processing files 
    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

            for line in csvreader:
                full_data_rows_list.append(line) 

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    with open('event_datafile.csv', 'r', encoding = 'utf8') as f:
        print((sum(1 for line in f)), ('rows processed to event_datafile.csv file'))
    print('...')

def processing_data(session, file):
           
    session.execute(music_history_create)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            artist, firstname, gender, itemInSession, lastname, length, level, location, sessionId, song, userId = line
            music_history_data = (int(sessionId), int(itemInSession), artist, song, float(length))
            session.execute(music_history_insert,  music_history_data)    
    rows = session.execute("""SELECT COUNT(*) FROM music_history""")
    print('Data inserted into music_history table')
    for row in rows:
        print(row)
    print('...')
        
    session.execute(user_songs_create)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            artist, firstname, gender, itemInSession, lastname, length, level, location, sessionId, song, userId = line
            user_songs_data = (int(userId), int(sessionId), artist, song, firstname, lastname, int(itemInSession))
            session.execute(user_songs_insert,  user_songs_data)    
    rows = session.execute("""SELECT COUNT(*) FROM user_songs""")
    print('Data inserted into user_songs table')
    for row in rows:
        print(row)
    print('...')

           
    session.execute(history_song_create)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            artist, firstname, gender, itemInSession, lastname, length, level, location, sessionId, song, userId = line
            history_song_data = (song, firstname, lastname, int(userId))
            session.execute(history_song_insert,  history_song_data)
    rows = session.execute("""SELECT COUNT(*) FROM history_song""")
    print('Data inserted into history_song table')
    for row in rows:
        print(row)
    print('...')

    
def main():
     
    #cassandra cluster 
    try:
        cluster = Cluster(['127.0.0.1'])
        #session to establish connection and begin executing queries
        session = cluster.connect()  
    except Exception as e:
        print(e)
         
    #creating keyspace
    try:
        session.execute(keyspace_create)
    except Exception as e:
        print(e)

    #set keyspace
    try:
        keyspace = session.set_keyspace('udacity')
    except Exception as e:
        print(e)
  
    # pre processing data 
    pre_processing_data()
    file = 'event_datafile.csv'    
    
    # processing data for each query 
    processing_data(session, file)
   

    


if __name__ == "__main__":
    main()

