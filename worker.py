from bloom_filter import BloomFilter
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
bloom = BloomFilter(max_elements=100000000, error_rate=0.001)

search_term = sys.argv[1]
normalized_term = search_term.replace("+", "_")
index = f"{normalized_term}_index"
queue = f"{normalized_term}_queue"


def write_to_es(transcript, video_id):
    actions = []

    for map_of_text in transcript:
        words = map_of_text["text"]
        if words not in bloom:
            bloom.add(words)
            map_of_text["video_id"] = video_id
            actions.append(
                {
                    "_index": f"{index}",
                    "_type": "document",
                    "_source": map_of_text
                }
        )
    helpers.bulk(es, actions)


def should_process(transcript, video_id):
    MIN_WORDS = 1000
    MIN_UNIQUE_WORDS = 500

    # {'text': 'is that a million yeah oh this is a', 'start': 0.06, 'duration': 5.04}
    unique_words = set()
    num_words = 0
    num_of_chars = 0

    for doc in transcript:
        line = doc["text"]
        words = line.split()
        num_words += len(words)
        num_of_chars += sum([len(word) for word in words])
        unique_words.update(set(words))

    avg_unique_word_size = sum([len(word) for word in unique_words]) / len(unique_words)
    avg_word_size = num_of_chars / num_words

    quality_score = len(unique_words) / num_words * 100
    redis.set(name=video_id, value=avg_unique_word_size)
    print("https://youtube.com/watch?v=" + video_id)
    if num_words < MIN_WORDS:
        print("< MIN_WORDS")
        return False
    if len(unique_words) < MIN_UNIQUE_WORDS:
        print("< MIN_UNIQUE_WORDS")
        return False
    if avg_unique_word_size < 6.1:
        print("< avg_unique_word_size 6.1: {}".format(avg_unique_word_size))
        return False

    return True


def main():
    print(f"Starting worker on queue: {queue}")
    print(f"ES Index: {index}")
    print(f"{queue} size:{redis.llen(queue)}")

    processed = 0
    skipped = 0
    while True:
        print("processed: {} // skipped: {} // {} size:{}".format(processed, skipped, queue, redis.llen(queue)))
        video_id = redis.lpop(queue)
        if video_id is None:
            continue
        video_id = video_id.decode("utf-8")
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            # [{'text': 'is that a million yeah oh this is a', 'start': 0.06, 'duration': 5.04}]
            if not should_process(transcript, video_id=video_id):
                skipped += 1
                continue
            processed += 1
            write_to_es(transcript, video_id)
        except KeyboardInterrupt:
            print('SIGINT or CTRL-C detected. Exiting gracefully')
            exit(0)
        except Exception as ex:
            print(f"Something went wrong. Keep going! Err -> {ex}")

if __name__ == '__main__':
    main()

# https://www.youtube.com/results?search_query=political+debate
