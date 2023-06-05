class SqlQueries:

    events_staging_table_create = ("""
        CREATE TABLE "staging_events" (
            "artist" varchar(1000),
            "auth" varchar(1000),
            "firstname" varchar(1000),
            "gender" varchar(1000),
            "itemInSession" int,
            "lastName"  varchar(1000),
            "length" float,
            "level" varchar(1000),
            "location" varchar(1000),
            "method" varchar(1000),
            "page" varchar(1000),
            "registration" float,
            "sessionId" int,
            "song" varchar(1000),
            "status" int,
            "ts" bigint,
            "userAgent" varchar(1000),
            "userId" int
        );
    """)

    songs_staging_table_create = ("""
        CREATE TABLE "staging_songs" (
            "num_songs" int,
            "artist_id" varchar(1000),
            "artist_latitube" float,
            "artist_longitube" float,
            "artist_location" varchar(1000),
            "artist_name" varchar(1000),
            "song_id" varchar(1000),
            "title" varchar(1000),
            "duration" float,
            "year" int
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE "songplays" (
            "songplay_id" int primary key,
            "start_time" int,
            "user_id" int,
            "level" varchar(1000),
            "song_id" varchar(1000),
            "artist_id" varchar(1000),
            "session_id" int,
            "location" varchar(1000),
            "user_agent" varchar(1000)
        );
    """)

    song_table_create = ("""
        CREATE TABLE "songs" (
            "song_id" int primary key,
            "title" varchar(1000),
            "year" int,
            "artist_id" varchar(1000),
            "duration" float
        );
    """)

    users_table_create = ("""
        CREATE TABLE "users" (
            "user_id" int primary key,
            "first_name" varchar(1000),
            "last_name" varchar(1000),
            "gender" varchar(1000),
            "level" varchar(1000)
        );
    """)

    songs_table_create = ("""
        CREATE TABLE "songs" (
            "song_id" int primary key,
            "title" varchar(1000),
            "year" int,
            "artist_id" varchar(1000),
            "duration" float
        );
    """)

    artist_table_create = ("""
        CREATE TABLE "artists" (
            "artist_id" varchar(1000) primary key,
            "name" varchar(1000),
            "location" varchar(1000),
            "latitube" float,
            "longitube" float
        );
    """)

    time_table_create = ("""
        CREATE TABLE "times" (
            "start_time" int primary key,
            "hour" int,
            "day" int,
            "week" int,
            "month" int,
            "year" int,
            "weekday" int
        );
    """)

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)