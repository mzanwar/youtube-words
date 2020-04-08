import datetime
from time import sleep

import sys
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()

threshold = 1  # at least this many docs

search_term = sys.argv[1]

def is_word_indexed(word):
    result = es.search(index=f"{search_term}_index", body={"query": {"match": {"text": f"{word}"}}}, terminate_after=1)
    if result['hits']['total'] > 0:
        return True
    return False


def words_stats(words):
    total = len(words)
    indexed = 0
    missing = []
    for word in words:
        if is_word_indexed(word):
            indexed += 1
        else:
            missing.append(word)
    return (total, indexed, indexed / total, missing)


if __name__ == '__main__':
    files = ["google-words.txt", "words.txt", "popular.txt"]
    while True:
        for file in files:
            with open(file, 'r') as lines:
                words = [word for word in lines]
                stats = words_stats(words=words)
                with open("stats-2.txt", "a") as stats_file:
                    stats_file.write(f"{datetime.datetime.now()} {file} = total: {stats[0]} indexed: {stats[1]} {str(stats[2] * 100)}%")
                    stats_file.write("\n")
        sleep(20) # 5 mins
