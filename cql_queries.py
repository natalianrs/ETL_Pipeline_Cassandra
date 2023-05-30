# creating key space 

keyspace_create = """
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """


# Creating tables

music_history_create = """
    CREATE TABLE IF NOT EXISTS music_history
    (sessionId int, 
    itemInSession int,
    artist text, 
    song text, 
    length float,
    PRIMARY KEY(sessionId, itemInSession))
    """

user_songs_create = """
    CREATE TABLE IF NOT EXISTS user_songs
    (userId int, 
    sessionId int, 
    artist text, 
    song text, 
    firstName text, 
    lastName text, 
    itemInSession int,
    PRIMARY KEY((userId, sessionId), itemInSession))
    """

history_song_create = """
    CREATE TABLE IF NOT EXISTS history_song
    (song text, 
    firstName text, 
    lastName text, 
    userId int,
    PRIMARY KEY(song, userId))
    """

# Inserting data into tables 
music_history_insert = "INSERT INTO music_history (sessionId,itemInSession, artist, song, length) VALUES (%s, %s, %s, %s, %s)"  
user_songs_insert = "INSERT INTO user_songs (userId, sessionId, artist, song, firstName, lastName, itemInSession) VALUES (%s, %s, %s, %s, %s, %s, %s)"
history_song_insert = "INSERT INTO history_song (song, firstName, lastName, userId) VALUES (%s, %s, %s, %s)"


# Droping tables

music_history_drop = """ DROP TABLE IF EXISTS music_history """
user_songs_drop = """ DROP TABLE IF EXISTS user_songs """
history_song_drop = """ DROP TABLE IF EXISTS history_song """


