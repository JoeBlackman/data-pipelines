class SqlQueries:

    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    clean_up_duplicate_artists = """
    BEGIN
    -- Identify duplicates
    CREATE TEMP TABLE dup_artists AS
    SELECT artist_id
    FROM artists
    GROUP BY artist_id
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMP TABLE new_artists(LIKE artists);
    INSERT INTO new_artists
    SELECT DISTINCT *
    FROM artists
    WHERE artist_id IN (SELECT artist_id FROM dup_artists);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM artists
    WHERE artist_id IN (SELECT artist_id FROM dup_artists);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO artists
    SELECT *
    FROM new_artists;

    -- clean up
    DROP TABLE dup_artists;
    DROP TABLE new_artists;
    COMMIT;
    """

    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    clean_up_duplicate_songs = """
    BEGIN
    -- Identify duplicates
    CREATE TEMP TABLE dup_songs AS
    SELECT song_id
    FROM songs
    GROUP BY song_id
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMP TABLE new_songs(LIKE songs);
    INSERT INTO new_songs
    SELECT DISTINCT *
    FROM songs
    WHERE song_id IN (SELECT song_id FROM dup_songs);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM songs
    WHERE song_id IN (SELECT song_id FROM dup_songs);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO songs
    SELECT *
    FROM new_songs;

    -- clean up
    DROP TABLE dup_songs;
    DROP TABLE new_songs;
    COMMIT;
    """

    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    # a unique event can potentially be identified by userId, sessionId, and ts
    # (an event occuring at a specific time pertaining to a user action during a session)
    clean_up_duplicate_staging_events = """
    BEGIN
    -- Identify duplicates
    CREATE TEMPORARY TABLE dup_staging_events AS
    SELECT se."userId", se."sessionId", se.ts 
    FROM staging_events se
    GROUP BY se."userId", se."sessionId", se.ts 
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMPORARY TABLE new_staging_events(LIKE staging_events);
    INSERT INTO new_staging_events
    SELECT DISTINCT *
    FROM staging_events se
    WHERE (se."userId", se."sessionId", se.ts ) IN (SELECT dse."userId", dse."sessionId", dse.ts FROM dup_staging_events dse);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM staging_events se
    WHERE (se."userId", se."sessionId", se.ts ) IN (SELECT dse."userId", dse."sessionId", dse.ts FROM dup_staging_events dse);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO staging_events
    SELECT *
    FROM new_staging_events;

    -- clean up
    DROP TABLE dup_staging_events;
    DROP TABLE new_staging_events;
    COMMIT;
    """

    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    clean_up_duplicate_staging_songs = """
    BEGIN
    -- Identify duplicates
    CREATE TEMPORARY TABLE dup_staging_songs AS
    SELECT ss.song_id
    FROM staging_songs ss
    GROUP BY ss.song_id
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMPORARY TABLE new_staging_songs(LIKE staging_songs);
    INSERT INTO new_staging_songs
    SELECT DISTINCT *
    FROM staging_songs ss
    WHERE ss.song_id IN (SELECT dss.song_id FROM dup_staging_songs dss);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM staging_songs ss
    WHERE ss.song_id IN (SELECT dss.song_id FROM dup_staging_songs dss);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO staging_songs
    SELECT *
    FROM new_staging_songs;

    -- clean up
    DROP TABLE dup_staging_songs;
    DROP TABLE new_staging_songs;
    COMMIT;
    """

    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    clean_up_duplicate_time = """
    BEGIN
    -- Identify duplicates
    CREATE TEMP TABLE dup_time AS
    SELECT start_time
    FROM time
    GROUP BY start_time
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMP TABLE new_time(LIKE time);
    INSERT INTO new_time
    SELECT DISTINCT *
    FROM time
    WHERE start_time IN (SELECT start_time FROM dup_time);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM time
    WHERE start_time IN (SELECT start_time FROM dup_time);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO time
    SELECT *
    FROM new_time;

    -- clean up
    DROP TABLE dup_time;
    DROP TABLE new_time;
    COMMIT;
    """
    # 06/23/2022
    # source: https://stackoverflow.com/questions/37582261/deleting-duplicates-rows-from-redshift
    clean_up_duplicate_users = """
    BEGIN
    -- Identify duplicates
    CREATE TEMP TABLE dup_users AS
    SELECT user_id
    FROM users
    GROUP BY user_id
    HAVING COUNT(*) > 1;

    -- Extract one copy of each duplicate
    CREATE TEMP TABLE new_users(LIKE users);
    INSERT INTO new_users
    SELECT DISTINCT *
    FROM users
    WHERE user_id IN (SELECT user_id FROM dup_users);

    -- Remove all rows that were duplicated (all copies)
    DELETE FROM users
    WHERE user_id IN (SELECT user_id FROM dup_users);

    -- Insert the rest of the records that had no duplicates
    INSERT INTO users
    SELECT *
    FROM new_users;

    -- clean up
    DROP TABLE dup_users;
    DROP TABLE new_users;
    COMMIT;
    """
    
    copy_json_from_s3 = """
        COPY ${table}
        FROM s3://${s3_bucket}/${s3_key}
        ACCESS_KEY_ID ${access_key}
        SECRET_ACCESS_KEY ${secret_key}
        JSON ${json}
        REGION ${region}
    """

    copy_delimited_file_from_s3 = """
        COPY ${table}
        FROM s3://${s3_bucket}/${s3_key}
        ACCESS_KEY_ID ${access_key}
        SECRET_ACCESS_KEY ${secret_key}
        REGION ${region}
    """
    
    create_artists_table = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id   varchar(256)        NOT NULL,
            name        varchar(512)        NOT NULL,
            location    varchar(512),
            latitude    DECIMAL(5, 2),
            longitude   DECIMAL(5, 2),
            primary key(artist_id)
        )
        distkey(artist_id)
        sortkey(name);
    """)
    
    create_songs_table = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id     varchar(256)        NOT NULL,
            title       varchar(512)        NOT NULL,
            artist_id   varchar(256)        NOT NULL,
            year        INT4,
            duration    DECIMAL(9, 5)       NOT NULL,
            primary key(song_id)
        )
        distkey(song_id)
        sortkey(title);
    """)

    create_songplays_table = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id     BIGINT              IDENTITY(0,1),
            start_time      TIMESTAMP           NOT NULL,
            user_id         INT4                NOT NULL,
            level           varchar(256)        NOT NULL,
            song_id         varchar(256),
            artist_id       varchar(256),
            session_id      INT4, 
            location        varchar(256),
            user_agent      varchar(256),
            primary key(songplay_id)
        )
        distkey(songplay_id)
        sortkey(start_time);
    """)

    create_staging_events_table = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist              VARCHAR(256),
            auth                VARCHAR(256),
            firstName           VARCHAR(256),
            gender              CHAR,
            itemInSession       INT4,
            lastName            VARCHAR(256),
            lenth               DECIMAL(9, 5),
            level               VARCHAR(256),
            location            VARCHAR(256),
            method              VARCHAR(256),
            page                VARCHAR(256),
            registration        VARCHAR(15),
            sessionId           INT4,
            song                VARCHAR(256),
            status              INT4,
            ts                  INT8,
            userAgent           VARCHAR(256),
            userId              INT4
        );
    """)

    create_staging_songs_table = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs           INT4,
            artist_id           VARCHAR(256),
            artist_latitude     DECIMAL(5, 2),
            artist_longitude    DECIMAL(5, 2),
            artist_location     VARCHAR(512),
            artist_name         VARCHAR(512),
            song_id             VARCHAR(256),
            title               VARCHAR(512),
            duration            DECIMAL(9, 5),
            year                INT4
        );
    """)

    create_time_table = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time  TIMESTAMP           NOT NULL,
            hour        INT4                NOT NULL,
            day         INT4                NOT NULL,
            week        INT4                NOT NULL,
            month       INT4                NOT NULL,
            year        INT4                NOT NULL,
            weekday     INT4                NOT NULL,
            primary key(start_time)
        )
        distkey(month)
        sortkey(start_time);
    """)
    
    create_users_table = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id         INT4          NOT NULL, 
            first_name      varchar(256)  NOT NULL, 
            last_name       varchar(256)  NOT NULL, 
            gender          CHAR,
            level           varchar(256)  NOT NULL,
            primary key(user_id)
        )
        distkey(user_id)
        sortkey(last_name);
    """)

    drop_artists_table = "DROP TABLE IF EXISTS artists;"
    
    drop_songs_table = 'DROP TABLE IF EXISTS songs;'

    drop_songplays_table = 'DROP TABLE IF EXISTS songplays;'

    drop_staging_events_table = 'DROP TABLE IF EXISTS staging_events;'

    drop_staging_songs_table = 'DROP TABLE IF EXISTS staging_songs;'

    drop_time_table = 'DROP TABLE IF EXISTS time;'

    drop_users_table = 'DROP TABLE IF EXISTS users;'

    insert_artists_table = ("""
        INSERT INTO artists(
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
        )
        WITH uniq_staging_songs AS (
            SELECT 
                artist_id, 
                artist_name AS name, 
                artist_location AS 
                location, 
                artist_latitude AS latitude, 
                artist_longitude AS longitude, 
                ROW_NUMBER() OVER(PARTITION BY artist_id) AS rank
            FROM staging_songs
        )
        SELECT 
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
        FROM uniq_staging_songs
        WHERE rank = 1;
    """)    

    insert_songs_table = ("""
        INSERT INTO songs(
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        )
        WITH uniq_staging_songs AS (
            SELECT 
                song_id, 
                title, 
                artist_id, 
                year, 
                duration, 
                ROW_NUMBER() OVER(PARTITION BY song_id) AS rank
            FROM staging_songs
        )
        SELECT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM uniq_staging_songs
        WHERE rank = 1;
    """)
    
    insert_songplays_table = ("""
        INSERT INTO songplays(
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
        WITH uniq_staging_events AS (
            SELECT 
                (timestamp 'epoch' + se.ts::numeric / 1000 * interval '1 second') as start_time, 
                se.userId, 
                ss.song_id, 
                ss.artist_id, 
                se.level, 
                se.song, 
                se.artist, 
                se.sessionId, 
                se.location, 
                se.userAgent
            FROM staging_events se
            LEFT OUTER JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
            WHERE se.page = 'NextSong'
        )
        SELECT start_time, userId, level, song_id, artist_id, sessionId, location, userAgent
        FROM uniq_staging_events;
    """)

    insert_time_table = ("""
        INSERT INTO time(
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday
        )
        WITH uniq_start_times AS (
            SELECT 
                (timestamp 'epoch' + ts::numeric / 1000 * interval '1 second') as start_time, 
                ROW_NUMBER() OVER(PARTITION BY ts) AS rank
		    FROM public.staging_events
        )
        SELECT 
            start_time, 
            EXTRACT(HOUR FROM start_time) AS hour, 
            EXTRACT(DAY FROM start_time) AS day, 
            EXTRACT(WEEK FROM start_time) AS week, 
	        EXTRACT(MONTH FROM start_time) AS month, 
            EXTRACT(YEAR FROM start_time) AS year, 
            EXTRACT(DOW FROM start_time) AS weekday
        FROM uniq_start_times
        WHERE rank = 1;
    """)

    insert_users_table = ("""
        INSERT INTO users(
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
        )
        WITH uniq_staging_events AS (
            SELECT 
                userId, 
                firstName, 
                lastName, 
                gender, 
                level, 
                ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
            FROM staging_events
            WHERE userId IS NOT NULL
        )
        SELECT 
            userId, 
            firstName, 
            lastName, 
            gender, 
            level
        FROM uniq_staging_events
        WHERE rank = 1;
    """)

    test_artists_count = "SELECT COUNT(*) FROM artists;"

    # because i can see the sql create that insists on these values being not null
    # this test isn't exactly necessary since the insertion would fail
    # however, the rublic insisted on testing this so we'll do it anyway
    # I guess this is just how the QA would verify that the data requirement is met?
    test_artists_nulls = """
        SELECT COUNT(*)
        FROM artists
        WHERE 
            artist_id IS NULL OR 
            name IS NULL;
    """

    test_songs_count = "SELECT COUNT(*) FROM songs;"

    test_songs_nulls = """
        SELECT COUNT(*)
        FROM songs
        WHERE 
            song_id IS NULL OR
            title IS NULL OR
            artist_id IS NULL OR
            durataion IS NULL;
    """

    test_songplays_count = "SELECT COUNT(*) FROM songplays;"
    
    test_songplays_nulls = """
        SELECT COUNT(*) 
        FROM songplays
        WHERE 
            start_time IS NULL OR 
            user_id IS NULL OR 
            level IS NULL;
    """

    test_time_count = "SELECT COUNT(*) FROM time;"

    test_time_nulls = """
        SELECT COUNT(*)
        FROM time
        WHERE 
            start_time IS NULL OR
            hour IS NULL OR
            day IS NULL OR
            week IS NULL OR
            month IS NULL OR
            year IS NULL OR
            weekday IS NULL;
    """

    test_users_count = "SELECT COUNT(*) FROM users;"

    test_users_nulls = """
        SELECT COUNT(*)
        FROM users
        WHERE 
            user_id IS NULL OR 
            first_name IS NULL OR 
            last_name IS NULL OR 
            level IS NULL;
    """
