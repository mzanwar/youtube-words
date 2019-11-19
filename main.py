from youtube_transcript_api import YouTubeTranscriptApi
import redis
import requests
import re
import time
import sys
from bloom_filter import BloomFilter

pattern = "watch\?v=.*?(?=\")\""

youtube_prefix = "https://youtube.com/watch?v="
seed_video = "TTNhAWXaXIo"

redis = redis.Redis(host='localhost', port=6379, db=0)
queue = "view_id_queue"

bloom = BloomFilter(max_elements=1000000000, error_rate=0.0001)

def regex_ids(html):
    s = set()
    list_of_ids = re.findall(pattern, html)
    for id in list_of_ids:
        s.add(id.split("=")[1][:-1])
    return s

def scrape_ids_from(url):
    print(url)
    r = requests.get(url)
    ids = regex_ids(r.text)
    count = 0
    for id in ids:
        if id not in bloom:
            redis.rpush(queue, id)
            count += 1
            bloom.add(id)
    print("added:" + str(count) + " / " + str(len(ids)))

def main():
    seed_url = sys.argv[2]

    scrape_ids_from(seed_url)
    while True:
        try:
            video_ids = redis.lrange(queue, 0,100)
            if video_ids is None:
                continue
            for video_id in video_ids:

                video_id = video_id.decode("utf-8")
                print(f"working on video id {video_id}")
                scrape_ids_from(youtube_prefix + video_id)
        except:
            print("Something went wrong. Keep going!")

if __name__ == '__main__':
    main()

# https://www.youtube.com/results?search_query=political+debate


