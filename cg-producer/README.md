Kafka Call Graph Generator
==========================

Consumes PyPI information about a package from a kafka topic, and
produces its call graph into another kafka topic.

Usage
-----

A copy of PyCG is required. It should be stored in a directory named `pycg`.

```
>>> mkdir callgraphs
>>> docker build -t pycg-gen .
>>> docker run \
        --mount type=bind,source=$(pwd)/sources,target=<source_dir> \
        --net=host \
        -it pycg-gen <input_topic> <cg_topic> <error_topic> \
        <comma_separated_servers> <consumer_group> <sleep_timeout> \
        <source_dir> <poll_interval>
```

The list of parameters are:
- `<input_topic>`: The kafka topic from which PyPI packaging information
  will be consumed.
- `<cg_topic>`: The kafka topic into which the producer will store call graphs.
- `<error_topic>`: The kafka topic into which the producer will store call
  graph generation errors.
- `<comma_separated_servers>`: The list of kafka servers in use, separated by
  commas.
- `<consumer_group>`: The group into which the consumer is assigned.
- `<sleep_timeout>`: How long will the consumer sleep before trying to consume
  new data.
- `<source_dir>`: Directory where call graphs and source files will be stored.
- `<poll_interval>`: Kafka interval between polling.

Apart from storing the call graphs in the output topic, they will also be
stored in the `callgraphs` directory mounted on the docker image.

Testing
-------

Make sure kafka is downloaded and switch to its installation directory.

```
# Start server
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

# create topics
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic package_list
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic call_graphs
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic error

# add packages to package_list topic
echo '{"product": "numpy", "version": "1.18.2", "version_timestamp": "2000"}
    {"product": "sqlparse", "version": "0.3.1", "version_timestamp": "3000"}
    {"product": "numpy", "version": "100.18.2", "version_timestamp": "2000"}\n' | \
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic package_list

docker run \
    --mount type=bind,source=$(pwd)/sources,target=/sources \
    --net=host \
    -it pycg-gen package_list call_graphs error\
    localhost:9092 mygroup 1 /sources 30000
```

After exiting, the source code for the three packages should be downloaded on
the `sources/sources` directory and two call graphs should be stored under the
`sources/callgraphs` directory.
The call graphs should also stored in the `call_graphs` topic,
while an error regarding versioning should be stored in the `error` topic.
