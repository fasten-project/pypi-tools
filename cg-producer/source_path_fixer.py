#
# Copyright (c) 2018-2020 FASTEN.
#
# This file is part of FASTEN
# (see https://www.fasten-project.eu/).
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import argparse
import datetime
import json
import os

from kafka import KafkaConsumer, KafkaProducer


class PyPIConsumer:
    def __init__(self, in_topic, out_topic, err_topic,\
                    source_dir, bootstrap_servers, group, poll_interval):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.source_dir = source_dir
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.group = group
        self.poll_interval = poll_interval
        self.plugin_name = "Python Source Path Fixer"


    def consume(self):
        self.consumer = KafkaConsumer(
            self.in_topic,
            bootstrap_servers=self.bootstrap_servers,
            # consume earliest available messages
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group,
            # messages are raw bytes, decode
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_interval_ms=self.poll_interval
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: x.encode('utf-8')
        )
        count = 0
        for message in self.consumer:
            count +=1 
            self.consumer.commit()
            ercg = message.value
            if ercg["input"]["payload"].get("product"):
                print ("{}: Consuming {}:{}".format(
                    datetime.datetime.now(),
                    ercg["input"]["payload"]["product"],
                    ercg["input"]["payload"]["version"]
                ))

                product = ercg["input"]["payload"]["product"]
                version =  ercg["input"]["payload"]["version"]
                source_path = self.source_dir + "/{}/{}/{}/".format(
                    product[0], product, version)
                if os.path.exists(source_path+"__init__.py"):
                    for module in ercg["input"]["payload"]["modules"]["internal"]:
                        source_file = ercg["input"]["payload"]["modules"]["internal"][module]["sourceFile"]
                        break
                
                    file_path = source_path+ source_file
                    if not os.path.exists(file_path):
                        file_name = source_file.split("/")[0]
                        print("------")
                        print(source_file)
                        print(source_path,file_name)
                        # move_source(source_path,file_name)
                        print("------")

                       
                    else:
                        print("Unexpected Error")
            else:
                print(ercg["input"])
                print("errror")


            if count ==10:
                break


    def _produce_error(self):
        # produce error to kafka topic
        output = dict(
            plugin_name=self.plugin_name,
            input=self.release,
            created_at=self._get_now_ts(),
            err=self.error_msg
        )
        self.producer.send(self.err_topic, json.dumps(output))

def get_parser():
    parser = argparse.ArgumentParser(
        """
        Consume PyPI packages from a Kafka topic, fixes their source directory if neccesary, and produces them in an output topic.
        """
    )
    parser.add_argument(
        'in_topic',
        type=str,
        help="Kafka topic to read from."
    )
    # parser.add_argument(
    #     'out_topic',
    #     type=str,
    #     help="Kafka topic to write call graphs."
    # )
    # parser.add_argument(
    #     'err_topic',
    #     type=str,
    #     help="Kafka topic to write errors."
    # )
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
    # parser.add_argument(
    #     'sleep_time',
    #     type=int,
    #     help="Time to sleep inbetween each scrape (in sec)."
    # )
    parser.add_argument(
        'source_dir',
        type=str,
        help="Directory where source code and call graphs will be stored."
    )
    # parser.add_argument(
    #     'poll_interval',
    #     type=int,
    #     help="Kafka poll interval"
    # )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    in_topic = args.in_topic
    # out_topic = args.out_topic
    # err_topic = args.err_topic
    bootstrap_servers = args.bootstrap_servers
    group = args.group
    source_dir = args.source_dir
    poll_interval = 3000

    consumer = PyPIConsumer(
        in_topic, "changeME", "changeME",\
        source_dir, bootstrap_servers, group, poll_interval)

    while True:
        consumer.consume()

if __name__ == "__main__":
    main()

# python3 source_path_fixer.py fasten.MetadataDBPythonExtension.rebalanced.out samos:9092  pypi_test_group /mnt/fasten/pypi/pypi/sources