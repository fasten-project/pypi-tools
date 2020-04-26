Kafka Filter for PyPI
=====================

Consumes PyPI packaging information in the
[Warehouse](https://warehouse.readthedocs.io/) format from a kafka topic
and produces unique package-version tuples into another kafka topic.

Usage
=====

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
