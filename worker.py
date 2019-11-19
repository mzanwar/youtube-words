from youtube_transcript_api import YouTubeTranscriptApi
import redis
import sys
from elasticsearch import Elasticsearch, helpers

# Worker responsibility is such:
# 1. Pop video id
# 2. Get transcript
# 3. Push to ES (we could make this a task queue as well and see how well that goes)

redis = redis.Redis(host='localhost', port=6379, db=0)
es = Elasticsearch()

search_term = sys.argv[1]
normalized_term = search_term.replace("+", "_")
index = f"{normalized_term}_index"
queue = f"{normalized_term}_queue"


def write_to_es(transcript, video_id):
    actions = []

    for map_of_text in transcript:
        map_of_text["video_id"] = video_id
        actions.append(
            {
                "_index": f"{index}",
                "_source": map_of_text
            }
        )
    helpers.bulk(es, actions)


def main():
    print(f"Starting worker on queue: {queue}")
    print(f"ES Index: {index}")
    print(f"{queue} size:{redis.llen(queue)}")

    while True:
        video_id = redis.lpop(queue)
        if video_id is None:
            continue
        video_id = video_id.decode("utf-8")
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            write_to_es(transcript, video_id)
        except KeyboardInterrupt:
            print('SIGINT or CTRL-C detected. Exiting gracefully')
            exit(0)
        except Exception as ex:
            print(f"Something went wrong. Keep going!Err -> {ex}")

if __name__ == '__main__':
    main()

# https://www.youtube.com/results?search_query=political+debate
