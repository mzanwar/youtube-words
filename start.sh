#!/usr/bin/env bash

term=$1
worker_count=$2
#start main
python3 main.py $term &

for worker in $worker_count
do
    python3 worker.py $term &
done