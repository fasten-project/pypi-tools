Kafka Test Statistics Producer
==============================

Consumes PyPI information about packages from a kafka topic,
and produces statistics regarding its tests.

Usage
-----

```
>>> docker build -t test-stats .
>>> docker run \
        --mount type=bind,source=$(pwd)/sources,target=<source_dir> \
        --net=host \
        -it test-stats <input_topic> <out_topic> \
        <comma_separated_servers> <consumer_group> <sleep_timeout> \
        <source_dir> <poll_interval> <copy_sources>
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
- `<copy_sources>`: A flag that specifies whether sources should be copied to
  local storage.
