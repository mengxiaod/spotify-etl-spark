-- create database
CREATE DATABASE spotify_db;


-- create integration (build connection between S3 and snowflake)
CREATE OR REPLACE STORAGE INTEGRATION s3_init
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::204537390950:role/spotify-spark-snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-204537390950-us-east-1-an')
COMMENT = 'Creating connection to S3'; 

DESC INTEGRATION s3_init;


-- create csv file format 
CREATE OR REPLACE FILE FORMAT csv_fileformat
TYPE = CSV 
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'; -- If a field value is wrapped in double quotes, treat everything inside those quotes as a single field — even if it contains commas. (in album_name column, some name contains comma, this should not be treated as column delimiter)

-- create staging
CREATE OR REPLACE STAGE spotify_stage
URL = 's3://spotify-etl-project-204537390950-us-east-1-an/transformed_data/'
STORAGE_INTEGRATION = s3_init
FILE_FORMAT = csv_fileformat;

LIST @spotify_stage;

-- CREATE table schemas
CREATE OR REPLACE TABLE tbl_album (
album_id STRING,
album_name STRING,
release_date DATE,
total_tracks INT,
url STRING
);

CREATE OR REPLACE TABLE tbl_artists (
artist_id STRING,
artist_name STRING,
external_url STRING
);

CREATE OR REPLACE TABLE tbl_songs (
song_id STRING,
song_name STRING,
duration_ms INT,
url STRING,
popularity INT,
song_added DATE,
album_id STRING,
artist_id STRING
);


-- copy test to check whether the table schema is all good
COPY INTO tbl_songs
FROM @spotify_stage/songs_data/songs_transformed_2026-05-22/run-1779487379248-part-r-00000;

COPY INTO tbl_artists
FROM @spotify_stage/artist_data/artist_transformed_2026-05-22/run-1779487378937-part-r-00000;

COPY INTO tbl_album
FROM @spotify_stage/album_data/album_transformed_2026-05-22/run-1779487378543-part-r-00000;

SELECT * FROM tbl_album;

-- create snow pipe
CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs_data;

CREATE OR REPLACE pipe pipe.tbl_artists_pipe
auto_ingest = TRUE
AS
COPY INTO public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist_data;

CREATE OR REPLACE pipe pipe.tble_album_pipe
auto_ingest = TRUE
AS
COPY INTO public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data;

DESC pipe pipe.tbl_songs_pipe;  -- find the notification_channel for setup S3 bucket SQS
DESC pipe pipe.tbl_artists_pipe;
DESC pipe pipe.tble_album_pipe;

-- upload new data into s3 bucket and to check here to see whether data auto loaded into table
SELECT COUNT(*) FROM public.tbl_songs;
SELECT COUNT(*) FROM public.tbl_artists;
SELECT COUNT(*) FROM public.tbl_album;

-- check pipe status
SELECT SYSTEM$PIPE_STATUS('pipe.tbl_album_pipe');
