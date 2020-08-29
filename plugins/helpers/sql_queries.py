class SqlQueries:
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
                FROM (SELECT TIMESTAMP 'epoch' + ts::numeric/1000 * INTERVAL '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') AS events
            LEFT JOIN staging_songs songs
                ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    # user_table_insert = ("""
    #     SELECT distinct userid, firstname, lastname, gender, level
    #     FROM staging_events
    #     WHERE page='NextSong'
    # """)

    # For users table, filter the staging table to the most recent start_time,
    # to capture the most recent "level" status.
    # NOTE: Do not use WHERE page="NextSong" because the most recent page for
    # several users is "Logout," so we would otherwise drop all those users
    user_table_insert = ("""
        SELECT
            DISTINCT events1.userid, events1.firstname, events1.lastname, events1.gender, events1.level
        FROM staging_events AS events1
        WHERE events1.userid IS NOT NULL
        AND events1.ts = (
            SELECT MAX(ts) 
            FROM staging_events AS events2
            WHERE events1.userid = events2.userid
            )
        ORDER BY events1.userid
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