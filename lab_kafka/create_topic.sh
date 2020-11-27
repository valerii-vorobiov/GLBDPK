#!/bin/bash
cd /usr/lib/kafka/bin || exit
bash kafka-topics.sh --zookeeper localhost:2181 --create --topic "$1" --partitions 1 --replication-factor 3
