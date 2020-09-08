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
import os
import json
import time
import kafka
import shutil
import argparse
import datetime
import subprocess as sp

from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer


class CallGraphGenerator:
    def __init__(self, out_topic, err_topic, producer, release):
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.producer = producer
        self.release = release

        self.release_msg = release
        self.product = release['product']
        self.version = release['version']
        self.version_timestamp = release['version_timestamp']
        self.requires_dist = release["requires_dist"]

        self.plugin_name = "PyCG"
        self.plugin_version = "0.0.1"

        # elapsed time for generating call graph
        self.elapsed = None
        # lines of code of package
        self.loc = None
        # maximum resident set size
        self.max_rss = None
        # number of files in the package
        self.num_files = None

        self.out_root = Path("pycg-data")
        self.out_dir = self.out_root/self.product/self.version
        if not self.out_dir.exists():
            self.out_dir.mkdir(parents=True)

        self.out_file = self.out_dir/'cg.json'

        self.downloads_dir = self.out_root/"downloads"
        self.untar_dir = self.out_root/"untar"

        self.error_msg = {
            'phase': '',
            'message': ''
        }

    def generate(self):
        try:
            comp = self._download()
            package = self._decompress(comp)
            cg_path = self._generate_callgraph(package)
            self._produce_callgraph(cg_path)
            self._unlink_callgraph(cg_path)
        except CallGraphGeneratorError:
            self._produce_error()
        finally:
            self._clean_dirs()

    def _get_now_ts(self):
        return int(datetime.datetime.now().timestamp())

    def _download(self):
        # Download tar into self.downloads_dir directory
        # return compressed file location
        err_phase = 'download'

        self._create_dir(self.downloads_dir)
        cmd = [
            'pip3',
            'download',
            #'--no-binary=:all:',
            '--no-deps',
            '-d', self.downloads_dir.as_posix(),
            "{}=={}".format(self.product, self.version)
        ]
        try:
            out, err = self._execute(cmd)
        except Exception as e:
            self._format_error(err_phase, str(e))
            raise CallGraphGeneratorError()

        items = list(self.downloads_dir.iterdir())
        if len(items) != 1:
            self._format_error(err_phase,\
                'Expecting a single downloaded item {}'.format(str(items)))
            raise CallGraphGeneratorError()

        return items[0]

    def _decompress(self, comp_path):
        # decompress `comp` and return the decompressed location
        err_phase = 'decompress'

        self._create_dir(self.untar_dir)
        file_ext = comp_path.suffix

        # TODO: More filetypes may exist
        if file_ext == '.gz':
            cmd = [
                'tar',
                '-xvf', comp_path.as_posix(),
                '-C', self.untar_dir.as_posix()
            ]
        elif file_ext == '.zip':
            cmd = [
                'unzip',
                '-d', self.untar_dir.as_posix(),
                comp_path.as_posix()
            ]
        elif file_ext == '.whl':
            zip_name = comp_path.with_suffix(".zip")
            try:
                comp_path.replace(zip_name)
            except Exception as e:
                self._format_error(err_phase, str(e))

            cmd = [
                'unzip',
                '-d', self.untar_dir.as_posix(),
                zip_name.as_posix()
            ]
        else:
            self._format_error(err_phase, 'Invalid extension {}'.format(file_ext))
            raise CallGraphGeneratorError()

        try:
            out, err = self._execute(cmd)
        except Exception as e:
            self._format_error(err_phase, str(e))
            raise CallGraphGeneratorError()

        # remove non python dirs extracted from '.whl'
        if file_ext == ".whl":
            dirs = [d for d in self.untar_dir.iterdir() if d.is_dir()]
            for d in dirs:
                nfiles = len(list(d.glob('**/*.py')))
                if nfiles == 0:
                    shutil.rmtree(d.as_posix())

        items = list(self.untar_dir.iterdir())
        if len(items) != 1:
            # return the item with the same name as the product
            prod_replaced = ''
            if self.product:
                prod_replaced = self.product.replace('-', '_')
            for item in items:
                if self.untar_dir/self.product == item:
                    return item
                # try with - replaced with _ (a common practice)
                if self.untar_dir/prod_replaced == item:
                    return item
            self._format_error(err_phase,\
                'Expecting a single item to be untarred or matching product name: {}'.format(str(items)))
            raise CallGraphGeneratorError()

        return items[0]

    def _generate_callgraph(self, package_path):
        # call pycg using `package`

        files_list = self._get_python_files(package_path)

        # get metrics from the files list
        self.num_files = len(files_list)
        self.loc = self._get_lines_of_code(files_list)

        # if the package path contains an init file
        # then the package is its parent
        if (package_path/"__init__.py").exists():
            package_path = package_path.parent

        cmd = [
            'pycg',
            '--fasten',
            '--package', package_path.as_posix(),
            '--product', self.product,
            '--version', self.version,
            '--forge', 'PyPI',
            '--timestamp', str(self.version_timestamp),
            '--output', self.out_file.as_posix()
        ] + files_list

        timing = [
            "/usr/bin/time",
            "-f", "secs=%e\nmem=%M"
        ]

        try:
            out, err = self._execute(timing + cmd)
        except Exception as e:
            self._format_error('generation', str(e))
            raise CallGraphGeneratorError()

        if not self.out_file.exists():
            self._format_error('generation', err.decode('utf-8'))
            raise CallGraphGeneratorError()

        for l in err.decode('utf-8').splitlines():
            if l.strip().startswith("secs"):
                self.elapsed = float(l.split("=")[-1].strip())
            if l.strip().startswith("mem"):
                self.max_rss = int(l.split("=")[-1].strip())

        return self.out_file

    def _get_python_files(self, package):
        return [x.resolve().as_posix().strip() for x in package.glob("**/*.py")]

    def _get_lines_of_code(self, files_list):
        res = 0
        for fname in files_list:
            with open(fname) as f:
                try:
                    res += sum(1 for l in f if l.rstrip())
                except UnicodeDecodeError as e:
                    continue

        return res

    def _produce_callgraph(self, cg_path):
        # produce call graph to kafka topic
        if not cg_path.exists():
            self._format_error('producer',\
                'Call graph path does not exist {}'.format(cg_path.as_posix()))
            raise CallGraphGeneratorError()

        with open(cg_path.as_posix(), "r") as f:
            try:
                cg = json.load(f)
            except Exception:
                self._format_error('producer',\
                    'Call graph path does is not JSON formatted {}. Contents {}'.format(cg_path.as_posix(), f.read()))
                raise CallGraphGeneratorError()

        # PyCG must produce a call graph with the key "depset"
        # The key "depset" must have the same format that comes from the input topic
        # if we have a requires_dist from the input topic
        # replace the one that pycg found from the requirements.txt file
        if len(self.requires_dist):
            cg["depset"] = self.requires_dist

        if not cg.get("metadata"):
            cg["metadata"] = {}

        cg["metadata"]["loc"] = self.loc or -1
        cg["metadata"]["time_elapsed"] = self.elapsed or -1
        cg["metadata"]["max_rss"] = self.max_rss or -1
        cg["metadata"]["num_files"] = self.num_files or -1

        output = dict(
                payload=cg,
                plugin_name=self.plugin_name,
                plugin_version=self.plugin_version,
                input=self.release,
                created_at=self._get_now_ts()
        )

        self.producer.send(self.out_topic, json.dumps(output))

    def _unlink_callgraph(self, cg_path):
        if not cg_path.exists():
            self._format_error('deleter',
                'Call graph path does not exist {}'.format(cg_path.as_posix()))
            raise CallGraphGeneratorError()
        cg_path.unlink()

    def _produce_error(self):
        # produce error to kafka topic
        output = dict(
            plugin_name=self.plugin_name,
            plugin_version=self.plugin_version,
            input=self.release,
            created_at=self._get_now_ts(),
            err=self.error_msg
        )
        self.producer.send(self.err_topic, json.dumps(output))

    def _execute(self, opts):
        cmd = sp.Popen(opts, stdout=sp.PIPE, stderr=sp.PIPE)
        return cmd.communicate()

    def _format_error(self, phase, message):
        self.error_msg['phase'] = phase
        self.error_msg['message'] = message
        print (self.error_msg)

    def _clean_dirs(self):
        # clean up directories created
        if self.downloads_dir.exists():
            shutil.rmtree(self.downloads_dir.as_posix())
        if self.untar_dir.exists():
            shutil.rmtree(self.untar_dir.as_posix())
        if self.out_root.exists():
            shutil.rmtree(self.out_root.as_posix())

    def _create_dir(self, path):
        if not path.exists():
            path.mkdir(parents=True)

class CallGraphGeneratorError(Exception):
    pass

class PyPIConsumer:
    def __init__(self, in_topic, out_topic, err_topic,\
                    bootstrap_servers, group, poll_interval):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.group = group
        self.poll_interval = poll_interval

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

        for message in self.consumer:
            self.consumer.commit()
            release = message.value
            print ("{}: Consuming {}".format(
                datetime.datetime.now(),
                release
            ))

            generator = CallGraphGenerator(self.out_topic, self.err_topic, self.producer, release)
            generator.generate()

def get_parser():
    parser = argparse.ArgumentParser(
        """
        Consume packages from a Kafka topic and produce their call graphs
        and generation errors into different Kafka topics.
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
    poll_interval = args.poll_interval

    consumer = PyPIConsumer(
        in_topic, out_topic, err_topic,\
        bootstrap_servers, group, poll_interval)

    while True:
        consumer.consume()
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
