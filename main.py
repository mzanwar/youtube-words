import redis
import requests
import re
import sys
from bloom_filter import BloomFilter

redis = redis.Redis(host='localhost', port=6379, db=0)
bloom = BloomFilter(max_elements=1000000000, error_rate=0.0001)

pattern = "watch\?v=.*?(?=\")\""
youtube_prefix = "https://youtube.com/watch?v="

search_term = sys.argv[1]
normalized_term = search_term.replace("+", "_")
index = f"{normalized_term}_index"
queue = f"{normalized_term}_queue"

seed_url = f"https://www.youtube.com/results?search_query={search_term}&sp=CAMSBggFEAEoAQ%253D%253D"


def regex_ids(html):
    s = set()
    list_of_ids = re.findall(pattern, html)
    for id in list_of_ids:
        s.add(id.split("=")[1][:-1])
    return s


def scrape_ids_from(url):
    r = requests.get(url)
    ids = regex_ids(r.text)
    count = 0
    for id in ids:
        if id not in bloom:
            redis.rpush(queue, id)
            count += 1
            bloom.add(id)


def main():
    print(f"Starting Youtube scan with term: {search_term}")
    print(f"Queue: {queue}")
    print(f"ES Index: {index}")
    print(f"{queue} size:{redis.llen(queue)}")
    print(f"Seed URL: {seed_url}")

    scrape_ids_from(seed_url)
    while True:
        scan_size = 20
        start = 0
        end = scan_size
        try:
            print(f"Scanning queue {queue} from {start} to {end}")
            print(f"{queue} size:{redis.llen(queue)}")

            video_ids = redis.lrange(queue, start, end)
            start += scan_size
            end += scan_size
            if video_ids is None:
                continue
            for video_id in video_ids:
                video_id = video_id.decode("utf-8")
                scrape_ids_from(youtube_prefix + video_id)
        except KeyboardInterrupt:
            print('SIGINT or CTRL-C detected. Exiting gracefully')
            exit(0)
        except Exception as ex:
            print(f"Something went wrong. Keep going!Err -> {ex}")


if __name__ == '__main__':
    main()

# https://www.youtube.com/results?search_query=basketball&sp=CAMSBggFEAEoAQ%253D%253D
