Kafka Call Graph Generator
==========================

Pulls a list of packages from a kafka topic
and generates their call graphs.
Call graphs and errors are put into another kafka topic.

Usage:

```
>>> docker build -t pycg-gen .
>>> docker run \
        --mount type=bind,source=$(pwd)/callgraphs,target=/cggen/callgraphs \
        --net=host \
        -it pycg-gen <input_topic> <cg_topic> <error_topic>\
        <comma_separated_servers> <consumer_group> <sleep_timeout>
```

Testing
-------

Make sure kafka is downloaded and switch to its directory.
```
# Start server
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

# create topics
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic package_list
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic call_graphs
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic error

# add packages to package_list topic
echo '{"product": "numpy", "version": "1.18.2", "version-timestamp": "2000"}\n{"product": "sqlparse", "version": "0.3.1", "version-timestamp": "3000"}\n"product": "numpy", "version": "100.18.2", "version-timestamp": "2000"}\n' | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic package_list

docker run \
    --mount type=bind,source=$(pwd)/callgraphs,target=/cggen/callgraphs \
    --net=host \
    -it pycg-gen package_list call_graphs error\
    localhost:9092 mygroup 1
```
