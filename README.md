# Spotify ETL Pipeline — AWS Lambda · PySpark · Snowflake

## Project Review

This project builds a fully automated ETL pipeline that continuously extracts Spotify playlist data, transforms it into a structured format, and loads it into a Snowflake data warehouse for analytics. It supports **two orchestration options**: a fully serverless AWS-native flow and a locally-hosted Apache Airflow DAG.

Raw playlist data is fetched from the **Spotify API** via **AWS Lambda** (Python), stored as JSON in **Amazon S3**, and transformed into three normalised tables — albums, artists, and songs — using **PySpark on AWS Glue**. The cleaned CSVs are then automatically ingested into **Snowflake** through **Snowpipe**, triggered by S3 event notifications.

**Key capabilities:**
- Two orchestration options — AWS-native (EventBridge + Lambda chain) or Apache Airflow DAG
- PySpark transformation on AWS Glue — flattens nested JSON, deduplicates, and splits into album, artist, and song datasets
- Auto-ingest with Snowpipe — new S3 files flow into Snowflake tables automatically via SQS notifications
- Raw data lifecycle management — processed JSON is moved from `to_processed/` to `processed/` after each Glue run
- Three normalised Snowflake tables ready for analytics queries

![](images/spotify_etl.png)

---

## S3 Folder Structure

```
s3://your-bucket/
├── raw_data/
│   ├── to_processed/      ← Lambda writes raw JSON here; Glue reads from here
│   └── processed/         ← Glue moves JSON here after transformation
└── transformed_data/
    ├── album_data/        ← album CSVs (partitioned by run date)
    ├── artist_data/       ← artist CSVs
    └── songs_data/        ← song CSVs
```

---

## Snowflake Schema

<table>
<tr>
<td>

**`tbl_album`**

| Column | Type |
|--------|------|
| `album_id` | STRING |
| `album_name` | STRING |
| `release_date` | DATE |
| `total_tracks` | INT |
| `url` | STRING |

</td>
<td>

**`tbl_artists`**

| Column | Type |
|--------|------|
| `artist_id` | STRING |
| `artist_name` | STRING |
| `external_url` | STRING |

</td>
<td>

**`tbl_songs`**

| Column | Type |
|--------|------|
| `song_id` | STRING |
| `song_name` | STRING |
| `duration_ms` | INT |
| `url` | STRING |
| `popularity` | INT |
| `song_added` | DATE |
| `album_id` | STRING |
| `artist_id` | STRING |

</td>
</tr>
</table>

---

## Orchestration Options

### Option 1 — AWS-Native (EventBridge · Lambda · Glue chain)

Amazon EventBridge triggers the Lambda function on a schedule. At the end of each Lambda run, `boto3` immediately starts the Glue job, and when Glue finishes writing CSVs, S3 event notifications fire Snowpipe automatically. No external orchestrator is required — the pipeline is entirely serverless.

### Option 2 — Apache Airflow DAG

The DAG [`spotify_lambda_trigger`](https://github.com/mengxiaod/airflow-local/blob/main/dags/spotify_etl_orchestration.py) runs on a local Airflow instance and coordinates the same AWS resources through three sequential tasks:

| # | Task | Operator | What it does |
|---|------|----------|--------------|
| 1 | `invoke_lambda` | `LambdaInvokeFunctionOperator` | Calls the `spotify_api_data_extract` Lambda to pull data from the Spotify API and write raw JSON to S3 |
| 2 | `s3_sensor` | `S3KeySensor` | Polls the S3 bucket every 60 s (up to 1 h) until the raw file appears, ensuring the Glue job only starts after data has landed |
| 3 | `run_glue_job` | `GlueJobOperator` | Launches `spotify_transformation_job` on AWS Glue to run the PySpark transformation |

Snowpipe handles the final load into Snowflake automatically once the Glue CSVs are written — the same as Option 1.

**When to choose Airflow:** use this option when you want explicit task-level visibility, retry policies, and a UI to monitor each step, or when you are already running Airflow for other pipelines and want to centralise scheduling there.

The DAG run below shows all three tasks completing successfully:

![Airflow DAG — all three tasks succeeded](images/airflow_dag.png)

---

## ETL Process

### Step 1 — Extract (AWS Lambda)

Lambda is triggered by EventBridge on a schedule and pulls the full playlist from the Spotify API.

- Authenticates with the Spotify API using `spotipy` and OAuth credentials stored as Lambda environment variables
- Fetches all tracks from the target playlist (`PLAYLIST_URI`) with market set to `US`
- Serialises the raw response as a timestamped JSON file and writes it to `raw_data/to_processed/` in S3
- Immediately triggers the AWS Glue job via `boto3` so transformation begins as soon as the raw file lands

---

### Step 2 — Transform (AWS Glue · PySpark)

The Glue job reads all JSON files from `raw_data/to_processed/`, flattens the nested structure, and produces three clean DataFrames.

**Albums:**
- Explodes the `items` array and selects `album.id`, `album.name`, `release_date`, `total_tracks`, and `external_urls.spotify`
- Drop deduplicates by `album_id` — the same album can appear across many tracks

**Artists:**
- Double-explodes `items` then the inner `artists` array to get one row per artist per track
- Selects `artist.id`, `artist.name`, and `external_urls.spotify`
- Drop deduplicates by `artist_id`

**Songs:**
- Double-explodes `items` and `artists` to get one row per song–artist pair
- Selects `song.id`, `song.name`, `duration_ms`, `url`, `popularity`, `added_at`, `album.id`, and `artist.id`
- Parses `added_at` as a `DATE` column (`song_added`)
- Drop deduplicates by `song_id`

---

### Step 3 — Load transformed data to S3 + raw data cleanup

**Write CSVs:**
- Each DataFrame is written as CSV to a date-partitioned path under `transformed_data/`

**Raw data lifecycle:**
- After writing the CSVs, the Glue job lists all JSON files in `raw_data/to_processed/`
- Each file is copied to `raw_data/processed/` then deleted from `to_processed/`
- This ensures the next Glue run does not reprocess already-handled data

---

### Step 4 — Auto-ingest into Snowflake (Snowpipe)

New CSV files in `transformed_data/` automatically trigger Snowpipe via S3 Event Notifications.

- Each S3 prefix has a dedicated SQS notification that maps to a Snowpipe pipe:
  - `album_data/` → `pipe.tbl_album_pipe` → `public.tbl_album`
  - `artist_data/` → `pipe.tbl_artists_pipe` → `public.tbl_artists`
  - `songs_data/` → `pipe.tbl_songs_pipe` → `public.tbl_songs`
- Snowpipe uses `COPY INTO` with the `csv_fileformat` definition (comma-delimited, header skipped, nulls handled, quoted fields supported)
- No manual load step required — data appears in Snowflake within seconds of the Glue job completing

---

## Setup

### 1. Lambda Function

1. **Environment Variables**

   | Key | Value |
   |-----|-------|
   | `SPOTIFY_CLIENT_ID` | your Spotify client ID |
   | `SPOTIFY_CLIENT_SECRET` | your Spotify client secret |
   | `SPOTIFY_REFRESH_TOKEN` | refresh token from `.cache` |

2. **Layer** — upload `spotipy_layer.zip`

3. **IAM Role** — attach `AmazonS3FullAccess` and `AWSGlueServiceRole`

4. **General Configuration** — set Timeout to `1 min 30 sec`

5. **Trigger** — EventBridge (CloudWatch Events)
   - Scheduled expression: `rate(1 minute)` *(for testing only — update before production)*


### 2. AWS Glue

1. **ETL Job** — create a Notebook job (`spotify_transformation_job`) using PySpark

2. **IAM Role** — attach the following policies:
   - `AmazonS3FullAccess`
   - `AWSGlueServiceNotebookRole`
   - `AWSGlueServiceRole`
   - `AWSLambda_FullAccess`
   - `IAMFullAccess`

3. **Worker** — type `G.1X`, 5 workers (Glue version 5.1)

4. **Data Flow**
   - Reads raw JSON from `raw_data/to_processed/`
   - Transforms into album, artist, and song DataFrames (deduplication applied)
   - Writes CSVs to `transformed_data/<entity>_data/<entity>_transformed_<date>/`
   - Copies processed JSON to `raw_data/processed/` and deletes originals


### 3. Apache Airflow

The DAG file lives in the companion repo: [mengxiaod/airflow-local](https://github.com/mengxiaod/airflow-local/blob/main/dags/spotify_etl_orchestration.py).



**Create the AWS connection** in the Airflow UI (Admin → Connections):

   | Field | Value |
   |-------|-------|
   | Connection ID | `aws_s3_spotify` |
   | Connection Type | Amazon Web Services |
   | AWS Access Key ID | your IAM key |
   | AWS Secret Access Key | your IAM secret |
   | Extra | `{"region_name": "us-east-1"}` |


---

### 4. Snowflake & Snowpipe

1. **IAM Role** — create a role with `AmazonS3FullAccess`; paste the generated `STORAGE_AWS_ROLE_ARN` and `STORAGE_AWS_EXTERNAL_ID` into `Spotify_snowpipe.sql`

2. **Run `Spotify_snowpipe.sql`** in order:
   - Create database `spotify_db`
   - Create storage integration `s3_init`
   - Create CSV file format and stage `spotify_stage`
   - Create tables `tbl_album`, `tbl_artists`, `tbl_songs`
   - Create Snowpipe pipes (`pipe.tbl_album_pipe`, `pipe.tbl_artists_pipe`, `pipe.tbl_songs_pipe`)

3. **SQS Triggers** — run `DESC PIPE pipe.<pipe_name>` to get each pipe's `notification_channel` ARN, then configure three S3 **Event Notifications** (one per entity) pointing to those SQS ARNs:

   | Prefix | Pipe |
   |--------|------|
   | `transformed_data/album_data/` | `pipe.tbl_album_pipe` |
   | `transformed_data/artist_data/` | `pipe.tbl_artists_pipe` |
   | `transformed_data/songs_data/` | `pipe.tbl_songs_pipe` |

4. **Verify** — manually upload a CSV to `transformed_data/` and confirm rows appear in the corresponding Snowflake table

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data source | Spotify Web API (`spotipy`) |
| Extraction | AWS Lambda (Python 3.13) |
| Orchestration (Option 1) | Amazon EventBridge · AWS Lambda (`boto3`) |
| Orchestration (Option 2) | Apache Airflow (local) · `LambdaInvokeFunctionOperator` · `S3KeySensor` · `GlueJobOperator` |
| Transformation | AWS Glue 5.1 · PySpark 3.5 |
| Storage | Amazon S3 |
| Loading | Snowpipe (auto-ingest via SQS) |
| Data warehouse | Snowflake |

---

## Local Development

```bash
# install dependencies
uv sync          # or: pip install -e .

# run extraction locally (requires .env with Spotify credentials)
python lambda_run_local.py

# prototype transformations
jupyter notebook spotify_etl.ipynb
```

`.env` file:
```
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
SPOTIFY_REFRESH_TOKEN=...
```

## Final Output

After the pipeline ran for 8 minutes, data was extracted from the Spotify API and loaded into Snowflake 5 times automatically. The AWS Glue job run history is shown below:

![AWS Glue job run history](images/AWS_glue_job.png)

The three resulting Snowflake tables:

**`tbl_album`**
![](images/tbl_album.png)

**`tbl_artists`**
![](images/tbl_artists.png)

**`tbl_songs`**
![](images/tbl_songs.png)
