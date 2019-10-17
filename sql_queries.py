import configparser

# Using config file to store AWS credentials and cluster details
# Using configparser library to extract the config details to be used within ETL process 
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLE STATEMENTS
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_max_ts_drop = "DROP TABLE IF EXISTS users_max_ts;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLE STATEMENTS

# Primary Key and Foreign Key are informational purpose only; Redshift doesn't implement them
# However, query optimizer uses this information in planning
# Varchar w/o length defaults to varchar(256)
# Char w/o length defaults to char(1)

""" 
  Events (log) files stored on AWS S3 will be copied to staging_events table using COPY function
  Data from this table is transformed and stored in other tables for analysis purpose
  Log files contain user activity on the app
  Given that the table is staging, setting table Backup to No; Snapshot will not backup this table
  Using Distribution Style by Key; Using Song attribute as distribution key
"""
staging_events_table_create= ("""CREATE TABLE staging_events(artist varchar
                                                           , auth varchar
                                                           , firstname varchar
                                                           , gender char
                                                           , iteminsession int
                                                           , lastname varchar
                                                           , length float
                                                           , level varchar
                                                           , location varchar
                                                           , method varchar
                                                           , page varchar
                                                           , registration bigint
                                                           , sessionid int
                                                           , song varchar           DISTKEY
                                                           , status int
                                                           , ts bigint
                                                           , useragent varchar
                                                           , userid int)
                                     BACKUP NO
                                     DISTSTYLE KEY;
                              """)

"""
  Songs files stored on AWS S3 will be copied to staging_songs table using COPY function
  Data from this table is transformed and stored in other tables for analysis purpose
  Song files contain metadata of the songs on app
  Given that the table is staging, setting table Backup to No; Snapshot will not backup this table
  Using Distribution Style of ALL in order to replicate table on all data nodes
    Considering songs table is small and won't grow as fast as log data, we can copy table on all nodes
"""
staging_songs_table_create = ("""CREATE TABLE staging_songs(num_songs int
                                                          , artist_id char(19)
                                                          , artist_latitude float
                                                          , artist_longitude float
                                                          , artist_location varchar
                                                          , artist_name varchar
                                                          , song_id char(19)
                                                          , title varchar
                                                          , duration float
                                                          , year  int)
                                 BACKUP NO
                                 DISTSTYLE ALL;
                              """)


"""
  Songplays is the Fact table in this Star schema
  songplay_id is an identity column starting at 1 with increment of 1
  Using song_id as distribution key and sort key to optimize songs play end-user queries
  Songs table is also designed to distribute table using song_id key to enable colocating with fact table key
"""
songplay_table_create = ("""CREATE TABLE songplays(songplay_id int                      IDENTITY(1, 1) PRIMARY KEY
                                                 , start_time timestamp                 NOT NULL REFERENCES time(start_time)
                                                 , user_id int                          NOT NULL REFERENCES users(user_id)
                                                 , level varchar                        NOT NULL
                                                 , song_id char(19)     SORTKEY DISTKEY NOT NULL REFERENCES songs(song_id)
                                                 , artist_id char(19)                   NOT NULL REFERENCES artists(artist_id)
                                                 , session_id int                       NOT NULL
                                                 , location varchar
                                                 , user_agent varchar                   NOT NULL)
                            DISTSTYLE KEY;
                         """)

"""
  Staging table to hold only latest user record based on max timestamp in events file
  Used in user_table_insert query to filter out older user records from events table
  This helps capture latest User info from the events file
"""
user_table_max_ts_create = ("""CREATE TABLE users_max_ts(userid int    SORTKEY NOT NULL
                                                       , mx_ts bigint          NOT NULL)
                               BACKUP NO
                               DISTSTYLE ALL; 
                            """)

""" 
  User dimension table
  Given that the table isn't big enough, using distribution style ALL
  Having table available on all compute nodes makes end-user queries efficient when joined with fact table
  Setting user_id as Sort Key enables redshift to skip blocks of data based on its values
"""
user_table_create = ("""CREATE TABLE users(user_id int         SORTKEY NOT NULL PRIMARY KEY
                                         , first_name varchar          NOT NULL
                                         , last_name varchar           NOT NULL
                                         , gender char
                                         , level varchar               NOT NULL)
                        DISTSTYLE ALL;
                     """)

""" 
  Songs dimension table
  Using song_id as distribution key so that Redshift colocates songs and songsplay table keys
  Although Songs table is small, distributing table using Key adds to end-user query optimization
  Setting song_id as Sort Key enables redshift to skip blocks of data based on its values when queried
"""
song_table_create = ("""CREATE TABLE songs(song_id char(19)    SORTKEY DISTKEY NOT NULL PRIMARY KEY
                                         , title varchar                       NOT NULL
                                         , artist_id char(19)                  NOT NULL
                                         , year int
                                         , duration float                      NOT NULL)
                        DISTSTYLE KEY;
                     """)

""" 
  Artist dimension table
  Given that the table isn't big enough, using distribution style ALL
  Having table available on all compute nodes makes end-user queries efficient when joined with fact table
  Setting artist_id as Sort Key enables redshift to skip blocks of data based on its values when queried
"""
artist_table_create = ("""CREATE TABLE artists(artist_id char(19)  SORTKEY NOT NULL PRIMARY KEY
                                             , name varchar                NOT NULL
                                             , location varchar
                                             , latitude float
                                             , longitude float)
                          DISTSTYLE ALL;
                       """)

""" 
  Time dimension table
  Depending upon the size of events data, this table could be big however, in current situation 
    distribution style ALL works well as it lowers the movement of data at query time
  Having table available on all compute nodes makes end-user queries efficient when joined with fact table
  Setting start_time as Sort Key enables redshift to skip blocks of data based on its values when queried
"""
time_table_create = ("""CREATE TABLE time(start_time timestamp  SORTKEY NOT NULL PRIMARY KEY 
                                        , hour int                      NOT NULL             
                                        , day int                       NOT NULL
                                        , week int                      NOT NULL
                                        , month int                     NOT NULL
                                        , year int                      NOT NULL
                                        , weekday int                   NOT NULL)
                        DISTSTYLE ALL;
                     """)


# STATEMENTS TO INSERT DATA TO STAGING TABLES AND STAR SCHEMA TABLES

"""
  Copying log data from S3
  Files are in JSON format
  JSON path file is needed as data source contains Camel case headers; Redshift only supports lowercase headers
  Source data path, IAM Role ARN, and JSON path file details are sourced from config file using config parser
"""
staging_events_copy = ("""COPY staging_events FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON '{}'
                          region 'us-west-2';
                       """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

"""
  Copying songs data from S3
  Files are in JSON format
  JSON Paths file not required as headers match Redshift format and can be mapped to table column names
  Source data path and IAM Role ARN details are sourced from config file using config parser
"""
staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         CREDENTIALS 'aws_iam_role={}'
                         FORMAT AS JSON 'auto'
                         region 'us-west-2';
                      """).format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

"""
  Songplays table is populated using Events and Songs staging tables
  timestamp is converted to date time type as redshift stores and processes it efficiently
  Page value of 'NextSong' is used to identify records of song plays
"""
songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id
                                                , session_id, location, user_agent)
                            SELECT DISTINCT timestamp 'epoch' + (e.ts/1000) * interval '1 second'
                                          , e.userid, e.level, s.song_id, s.artist_id, e.sessionid
                                          , e.location, e.useragent
                            FROM staging_events e
                            INNER JOIN staging_songs s
                              ON (s.title = e.song
                                  AND s.duration = e.length
                                  AND s.artist_name = e.artist)
                            WHERE e.page = 'NextSong';
                         """)

"""
  Using users_max_ts as temp table to store only the latest User records using max timestamp
"""                            
user_table_max_ts_insert = ("""INSERT INTO users_max_ts(userid, mx_ts)
                               SELECT userid, max(ts) mx_ts 
                               FROM staging_events 
                               WHERE userid IS NOT NULL
                               GROUP BY userid;
                            """)

"""
  staging_events table joined to users_max_ts table to identify and insert only latest User details   
"""
user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT e.userid, e.firstname, e.lastname, e.gender, e.level
                        FROM staging_events e
                        INNER JOIN users_max_ts mts
                                ON (e.userid = mts.userid
                                    AND e.ts = mts.mx_ts);
                     """)

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM staging_songs;
                     """)

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude
                                        , artist_longitude
                          FROM staging_songs;
                       """)

"""
  Redshift doesn't have any pre-defined method to convert string to timestamp
  After researching, came across a solution on Stackoverflow to use Interval '1 second' along with epoch
    https://stackoverflow.com/questions/31636304/convert-text-to-timestamp-in-redshift
    Redshift timestamp epoch equals 1970-01-01 00:00:00.000000
    Interval adds specified date, time, and day values to any datepart
    ts represents timestamp from events (log) files which is stored in staging_events table as BIGINT
    ts is in miliseconds hence it is divided by 1000 to convert to seconds and then multiplied 
        by "Interval '1 second'", resulting in the interval of ts seconds since epoch
    Result from interval calc is added to epoch which returns the proper Redshift Timestamp for ts
"""
time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT (timestamp 'epoch' + (ts/1000) * interval '1 second')
                                       , extract(hr from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                                       , extract(day from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                                       , extract(week from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                                       , extract(month from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                                       , extract(year from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                                       , extract(weekday from (timestamp 'epoch' + (ts/1000) * interval '1 second'))
                        FROM staging_events;
                     """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_max_ts_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_max_ts_drop, staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_max_ts_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
