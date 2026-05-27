# Spotify ETL Pipeline Project - AWS Lambda, PySpark, Snowflake

## S3 Folder Structure

```
s3://your-bucket/
├── raw_data/
│   ├── processed/
│   └── to_processed/
└── transformed_data/
    ├── album_data/
    ├── artist_data/
    └── songs_data/
```

## Lambda Function Setup

1. **Environment Variables**

   | Key | Value |
   |-----|-------|
   | `SPOTIFY_CLIENT_ID` | your Spotify client ID |
   | `SPOTIFY_CLIENT_SECRET` | your Spotify client secret |
   | `SPOTIFY_REFRESH_TOKEN` | refresh token from `.cache` |

2. **Layers** — upload `spotipy_layer.zip`

3. **IAM Role** — attach `AmazonS3FullAccess` policy

4. **General Configuration** — set Timeout to `1 min 30 sec`

5. **Trigger** — EventBridge (CloudWatch Events)
   - Create new rule
   - Scheduled expression: `rate(1 minute)` *(for testing only — update before production)*


## AWS Glue Setup

1. **ETL Job** — create a Notebook job using PySpark

2. **IAM Role** — attach the following policies:
   - `AmazonS3FullAccess`
   - `AWSGlueServiceNotebookRole`
   - `AWSGlueServiceRole`
   - `AWSLambda_FullAccess`
   - `IAMFullAccess`

3. **Data Flow** — Glue reads from `raw_data/to_processed/`, transforms the data, then writes results to `transformed_data/`


## Snowpipe

1. **IAM Role** — attach `AmazonS3FullAccess` for the S3–Snowflake connection; update `arn` and `externalId` accordingly

2. **Snowflake Setup** — create database, storage integration, staging area, table schema, and Snowpipe

3. **SQS Triggers** — configure three SQS event notifications under the S3 bucket **Properties** tab, one each to trigger Snowpipe for `album_data/`, `artist_data/`, and `songs_data/`

4. **Logic** — new files written to `transformed_data/` automatically flow into the corresponding Snowflake table via Snowpipe




## Automation

1. **AWS Lambda** 
   - add `boto3` Glue client to trigger the Glue job (transform data) each time after new raw data is extracted to S3 
2. **AWS Glue** 
   - add `boto3` S3 client to copy processed raw data (json file) into `raw/processed/` and delete the original raw data in `raw/to_processed/`.

3. **Lambda Trigger** — EventBridge (CloudWatch Events) - extract raw data from Spotify API every 1 minute
   - Scheduled expression: `rate(1 minute)` *(for testing only — update before production)*
