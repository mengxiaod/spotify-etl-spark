import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler

PLAYLIST_URI = "3FmE8EwSfN556Gbi9Rr7NF"


def lambda_handler(event, context):
    cache_handler = MemoryCacheHandler(token_info={
        "access_token": "",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
        "scope": "playlist-read-private playlist-read-collaborative",
        "expires_at": 0,
    })
    auth_manager = SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri="http://127.0.0.1:8080/callback",
        scope="playlist-read-private playlist-read-collaborative",
        cache_handler=cache_handler,
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)
    data = sp.playlist(PLAYLIST_URI, market="US")
    spotify_data = data["items"]
    print(spotify_data)

    return {"statusCode": 200, "body": "OK"}
