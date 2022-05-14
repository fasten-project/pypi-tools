PyPI Source Path Fixer
==========================

This tool aims to resolve the issue described [here](https://github.com/fasten-project/pypi-tools/issues/2).
More precisesly, it consumes the output of PyCG from a kafka topic,
and for each PyPI coordiante,
checks whether its source code has been placed under the correct directory on disk.
If that is not the case, it fixes the source Path.

Usage
-----


```
>>> docker build -t pypi-source-path-fix .
>>> docker run \
        --mount type=bind,source=$(pwd)/sources,target=<source_dir> \
        --net=host \
        -it pypi-source-path-fix <input_topic> <out_topic> <error_topic> \
        <comma_separated_servers> <consumer_group> <sleep_timeout> \
        <source_dir> <poll_interval>
```

The list of parameters are:
- `<input_topic>`: The kafka topic from which PyPI call graph information
  will be consumed.
- `<out_topic>`: The kafka topic into which the producer will store the fixed cooridnates.
- `<error_topic>`: The kafka topic into which the producer will store the errors.
- `<comma_separated_servers>`: The list of kafka servers in use, separated by
  commas.
- `<consumer_group>`: The group into which the consumer is assigned.
- `<sleep_timeout>`: How long will the consumer sleep before trying to consume
  new data.
- `<source_dir>`: Directory were the source code of the PyPI revisions exists.
- `<poll_interval>`: Kafka interval between polling.

Example Output
-----

```
{
  "plugin_name": "Python Source Path Fixer",
  "product": <packageName>,
  "version": <version>,
  "created_at": <timestamp>
}
```


Apart form fixing the source directory of some PyPI revisions,
it will also store those revisions to an output topic.