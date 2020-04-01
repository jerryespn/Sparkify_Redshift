# Sparkify - Data Engineer Nanodegree program Cloud Datawarehouse Project
# By JGEL
# March 2020

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# TO READ FROM S3 DATA
IAM_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_location VARCHAR,
        artist_longitude NUMERIC,
        artist_name VARCHAR,
        duration NUMERIC,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id INT NOT NULL,
        song_id VARCHAR NOT NULL distkey,
        artist_id VARCHAR NOT NULL sortkey,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY sortkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR 
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id VARCHAR PRIMARY KEY distkey,
        title VARCHAR,
        artist_id VARCHAR NOT NULL sortkey, 
        year INT NOT NULL, 
        duration NUMERIC NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id VARCHAR PRIMARY KEY sortkey,
        name VARCHAR distkey,
        location VARCHAR,
        latitude NUMERIC, 
        longitude NUMERIC
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(SONG_DATA, IAM_ARN)

# INSERTING DATA INTO TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        se.userid,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent
    FROM staging_events se 
    JOIN staging_songs ss 
        ON ss.artist_name = se.artist AND ss.title = se.song
    WHERE se.page = 'NextSong'
    AND ss.artist_name is not null
    AND ss.title is not null;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid, firstname, lastname, gender, level
    FROM staging_events
    WHERE userid IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, EXTRACT(hour from start_time),
    EXTRACT(day from start_time), EXTRACT(week from start_time),
    EXTRACT(month from start_time), EXTRACT(year from start_time), EXTRACT(weekday from start_time)
    FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]