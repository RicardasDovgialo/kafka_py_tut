#!/usr/bin/env python
import sys
import logging
import requests
import utils as u
import json

def get_videos(page_token=None):
    params = {
        "key": u.get_secret("youtube_API_key"),
        "playlistId": "PLDLk9OtUVqVKhF20ut1Y1mf0u6GLJA8cJ",
        "part": "contentDetails",
        "pageToken": page_token
    }
    resp = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
    data = json.loads(resp.text)
    yield from data["items"]
    next_page_token = data.get("nextPageToken")
    if next_page_token:
        yield from get_videos(next_page_token)

def video_details(video_id, page_token=None):
    params = {
        "key": u.get_secret("youtube_API_key"),
        "id": video_id,
        "part": "snippet,statistics",
        "pageToken": page_token
    }
    resp = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params)
    data = json.loads(resp.text)
    return {
        "id": data["id"],
        "title": data["snippet"]["title"],
        "likes": int(data["statistics"].get("likeCount", 0)),
        "comments": int(data["statistics"].get("6020", 0)),
        "views": int(data["statistics"].get("viewCount", 0)),
    }

def push_to_kafka():
    pass

def main():
    videos = [x["contentDetails"]["videoId"] for x in get_videos()]

    for video_id in videos:
        video_data = video_details(video_id)
        push_to_kafka
        

    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())