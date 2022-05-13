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
import time
import json
import os
import shutil
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

def move_source(init_path, intermediate_dir):
    files = os.listdir(init_path)
    dest_path = os.path.join(init_path, intermediate_dir)
    if intermediate_dir not in files:
        Path(dest_path).mkdir(parents=True,  exist_ok=True)
        for file in files:
            shutil.move(os.path.join(init_path,file), os.path.join(dest_path, file))
    else:
        # When the coordiante has at least one directory with the same name with the target one,
        # we use a temporary directory to store our source code in order to avoid a conflict between the 2 directories. 
        temp_dir = os.path.join("/tmp/fasten-sources/", intermediate_dir)
        Path(temp_dir).mkdir(parents=True,  exist_ok=True)
        for file in files:
            shutil.move(os.path.join(init_path,file), os.path.join(temp_dir, file))

        shutil.move(temp_dir, dest_path)
        
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

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
            if ercg["payload"].get("product"):
                print ("{}: Consuming {}:{}".format(
                    datetime.datetime.now(),
                    ercg["payload"]["product"],
                    ercg["payload"]["version"]
                ))

                product = ercg["payload"]["product"]
                version =  ercg["payload"]["version"]
                source_path = self.source_dir + "/{}/{}/{}/".format(
                    product[0], product, version)
                if os.path.exists(source_path+"__init__.py"):
                    for module in ercg["payload"]["modules"]["internal"]:
                        source_file = ercg["payload"]["modules"]["internal"][module]["sourceFile"]
                        break
                
                    file_path = source_path+ source_file
                    if not os.path.exists(file_path):
                        file_name = source_file.split("/")[0]
                        move_source(source_path, file_name)
                        print("Succesfully fixed source path of", product, version)
                        output = dict(
                        plugin_name=self.plugin_name,
                        product=product,
                        version=version,
                        created_at=self._get_now_ts())
                        self.producer.send(self.out_topic, json.dumps(output))
            else:
                output = dict(
                    plugin_name=self.plugin_name,
                    input=ercg,
                    type="Could not find product name",
                    created_at=self._get_now_ts())
                self.producer.send(self.err_topic, json.dumps(output))
    
    def _get_now_ts(self):
        return int(datetime.datetime.now().timestamp())


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
    parser.add_argument(
        'out_topic',
        type=str,
        help="Kafka topic to write call graphs."
    )
    parser.add_argument(
        'err_topic',
        type=str,
        help="Kafka topic to write errors."
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
        'source_dir',
        type=str,
        help="Directory where source code and call graphs will be stored."
    )
    parser.add_argument(
        'poll_interval',
        type=int,
        help="Kafka poll interval"
    )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    in_topic = args.in_topic
    out_topic = args.out_topic
    err_topic = args.err_topic
    bootstrap_servers = args.bootstrap_servers
    group = args.group
    sleep_time = args.sleep_time
    source_dir = args.source_dir
    poll_interval = args.poll_interval

    consumer = PyPIConsumer(
        in_topic, out_topic, err_topic,\
        source_dir, bootstrap_servers, group, poll_interval)

    while True:
        consumer.consume()
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()

# python3 source_path_fixer.py fasten.MetadataDBPythonExtension.rebalanced.out pypi.fix pypi.fix.err 172.16.45.120:9092,172.16.45.121:9092,172.16.45.122:9092  pypi_test_group 5 /mnt/fasten/pypi/sources 300000

#  kafka-run-class kafka.tools.GetOffsetShell --broker-list samos:9092 --offsets 1 --topic  pypi.fix | awk -F ':' '{sum += $3} END {print sum}'
#  kafka-consumer-groups --bootstrap-server samos:9092 --describe --group pypi_test
# python3 source_path_fixer.py fasten.pycg.cvt.out fasten.pypi.sourcePathFixer.out fasten.pypi.sourcePathFixer.err delft.ewi.tudelft.nl:9092,samos.ewi.tudelft.nl:9092,goteborg.ewi.tudelft.nl:9092 pypi_fix_group 5 /mnt/fasten/pypi/pypi/sources 600000
p