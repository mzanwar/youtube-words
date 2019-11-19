from youtube_transcript_api import YouTubeTranscriptApi
import redis
import requests
import re
import time
import sys

# Worker responsibility is such:
# 1. Pop video id
# 2. Get transcript
# 3. Push to ES (we could make this a task queue as well and see how well that goes)

redis = redis.Redis(host='localhost', port=6379, db=0)
queue = "view_id_queue"
es_index = sys.argv[1]

def main():
    while True:
        video_id = redis.lpop(queue)
        if video_id is None:
            continue
        video_id = video_id.decode("utf-8")
        try:
            resp = YouTubeTranscriptApi.get_transcript(video_id)
            print(resp)
        except:
            print("Something went wrong when getting subtitles. Keep going")


if __name__ == '__main__':
    main()

# https://www.youtube.com/results?search_query=political+debate


