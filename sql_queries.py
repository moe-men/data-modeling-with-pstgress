# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
create table songplays(
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id text NOT NULL,
    level varchar(20),
    song_id text,
    artist_id text,
    session_id integer,
    location text,
    user_agent text,
    UNIQUE ( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent )
);
""")

user_table_create = ("""
create table users(
    user_id text PRIMARY KEY,
    first_name text,
    last_name text,
    gender char(1),
    level varchar(20)
);
""")

song_table_create = ("""
create table songs(
    song_id text PRIMARY KEY,
    title text NOT NULL,
    artist_id text,
    year integer,
    duration double precision NOT NULL
);
""")

artist_table_create = ("""
create table artists(
    artist_id text PRIMARY KEY,
    name text NOT NULL,
    location text,
    latitude double precision,
    longitude double precision
);
""")

time_table_create = ("""
create table time(
    start_time timestamp PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer,
    UNIQUE ( start_time )
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays ( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent )
        values (%s , %s , %s , %s , %s , %s , %s , %s )
        ON CONFLICT (start_time , user_id , level , song_id , artist_id , session_id , location , user_agent) 
        DO NOTHING;
""")

user_table_insert = ("""
    insert into users ( user_id , first_name , last_name , gender , level )
        values (%s , %s , %s , %s , %s )
        ON CONFLICT (user_id) 
        DO UPDATE
            SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    insert into songs ( song_id , title , artist_id , year , duration )
        values (%s , %s , %s , %s , %s )
        ON CONFLICT (song_id) 
        DO UPDATE
            SET title  = EXCLUDED.title,
                artist_id  = EXCLUDED.artist_id,
                year  = EXCLUDED.year,
                duration  = EXCLUDED.duration;
""")

artist_table_insert = ("""
    insert into artists ( artist_id , name , location , latitude , longitude )
        values (%s , %s , %s , %s , %s )
        ON CONFLICT (artist_id) 
        DO UPDATE
            SET name  = EXCLUDED.name,
                location  = EXCLUDED.location,
                latitude  = EXCLUDED.latitude,
                longitude  = EXCLUDED.longitude;
""")


time_table_insert = ("""
    insert into time ( start_time , hour , day , week , month , year , weekday )
        values (%s , %s , %s , %s , %s , %s  , %s)
        ON CONFLICT (start_time)
        DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT 
    DISTINCT songs.song_id , artists.artist_id  
FROM songs join artists on songs.artist_id = artists.artist_id
WHERE songs.title = %s and artists.name = %s and songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]