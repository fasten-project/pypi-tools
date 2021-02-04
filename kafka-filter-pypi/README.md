Kafka Filter for PyPI
=====================

Consumes PyPI packaging information in the
[Warehouse](https://warehouse.readthedocs.io/) format from a kafka topic
and produces unique package-version tuples into another kafka topic.

Usage
-----

```
>>> docker build -t pypi-filter .
>>> docker run net=host -it pypi-filter\
        <input_topic> <out_topic> <comma_separated_servers>\
        <consumer_group> <sleep_timeout> [--check-old]
```

The list of parameters are:

- `<input_topic>`: The kafka topic from which PyPI packaging information will
  be consumed.
- `<out_topic>`: The kafka topic where the unique package-version tuples will
  be stored.
- `<comma_separated_servers>`: Thes list of kafka servers in use, separated by
  commas.
- `<consumer_group>`: The group into which the consumer for the input topic is
  assigned.
- `<sleep_timeout>`: How long will the consumer sleep before trying to consume
  new data.
- `--check-old`: If set, check the entries already stored into the output topic
  so no duplicates are added. Recommended to set.

Testing
-------

Make sure kafka is downloaded and switch to its installation directory.
We have included in this directory example input coordinates stored in
`example_input.json`.

```
# Start server
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &

# create topics
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic input_coords
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic filtered

# add packages to package_list topic
cat example_input.json | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input_coords

docker run -it pypi-filter input_coords filtered localhost:9092 \
    mygroup 1 --check-old
```

The output on the `filtered` kafka topic should be JSON records of the form
```json
{
    "product": <release>,
    "version": <version>,
    "version_timestamp": <version_timestamp>,
    "requires_dist": <requires_dist>
```

Each combination of `product` and `version` should be unique.
