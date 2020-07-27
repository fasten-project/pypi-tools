import time
import json
import argparse
import datetime
import dateutil.parser

from pkg_resources import Requirement

from kafka import KafkaConsumer, KafkaProducer

class CommitFilter:
    def __init__(self, in_topic, out_topic, bootstrap_servers, group, check_old):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.group = group
        self.check_old = check_old

        self.COMMIT_PHASE = "commit"

        self.releases = {}
        if self.check_old:
            self._fill_old()

    def consume(self):
        self._init_kafka()

        for message in self.consumer:
            release = message.value
            print ("{}: Consuming {} v{}".format(
                datetime.datetime.now(),
                release["input"]['product'],
                release["input"]['version']
            ))

            if release['err']['phase'] != self.COMMIT_PHASE:
                continue

            if not self._exists(release):
                self._store(release)
                self.produce(release)
            else:
                print ("{} already exists".format(release))

    def produce(self, entry):
        self.producer.send(self.out_topic, json.dumps(entry['input']))

    def _exists(self, entry):
        if not entry['input']["product"] in self.packages:
            return False
        if not entry['input']["version"] in self.packages[entry['input']["product"]]:
            return False
        return True

    def _store(self, entry):
        if not entry["input"]["product"] in self.packages:
            self.packages[entry["input"]["product"]] = set()
        self.packages[entry["input"]["product"]].add(entry["input"]["version"])

    def _init_kafka(self):
        self.consumer = KafkaConsumer(
            self.in_topic,
            bootstrap_servers=self.bootstrap_servers,
            # consume earliest available messages
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group,
            # messages are raw bytes, decode
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: x.encode('utf-8')
        )

    def _fill_old(self):
        consumer = KafkaConsumer(
            self.out_topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            # set timeout so we don't get stuck on old messages
            consumer_timeout_ms=1000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            release = message.value
            if release['err']['phase'] != self.COMMIT_PHASE:
                continue

            if not self._exists(release):
                self._store(release)


def get_parser():
    parser = argparse.ArgumentParser(
        """
        Consume PyPI packaging information from a Kafka topic and
        filter it to remove duplicate versioning information.
        Produce unique package-version tuples to the output topic.
        """
    )
    parser.add_argument(
        'in_topic',
        type=str,
        help="Kafka topic to read from."
    )
    parser.add_argument(
        'out_topic',
        type=str,
        help="Kafka topic to write unique package-version tuples."
    )
    parser.add_argument(
        'bootstrap_servers',
        type=str,
        help="Kafka servers, comma separated."
    )
    parser.add_argument(
        'group',
        type=str,
        help="Kafka consumer group to which the consumer belongs."
    )
    parser.add_argument(
        'sleep_time',
        type=int,
        help="Time to sleep inbetween each scrape (in sec)."
    )
    parser.add_argument(
        '--check-old',
        dest="check_old",
        action='store_true',
        default=False,
        help="Read messages from the output topic before consuming."
    )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    in_topic = args.in_topic
    out_topic = args.out_topic
    bootstrap_servers = args.bootstrap_servers
    group = args.group
    sleep_time = args.sleep_time
    check_old = args.check_old

    commit_filter = CommitFilter(
        in_topic, out_topic, bootstrap_servers,
        group, check_old)

    while True:
        commit_filter.consume()

        time.sleep(sleep_time)
        check_old = False

if __name__ == "__main__":
    main()
