# How to Setup S3 and Deploy Lambda

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