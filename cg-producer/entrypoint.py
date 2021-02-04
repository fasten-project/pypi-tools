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
import urllib
import kafka
import shutil
import argparse
import datetime
import subprocess as sp

from pathlib import Path
from distutils import dir_util

from kafka import KafkaConsumer, KafkaProducer

class CGConverter:
    """
    Converts FASTEN version 1 Call Graphs to Version 2
    """
    def __init__(self, cg):
        self.cg = cg
        self.new_cg = {
            "product": cg["product"],
            "forge": cg["forge"],
            "nodes": None,
            "generator": cg["generator"],
            "depset": cg["depset"],
            "version": cg["version"],
            "modules": {
                "internal": cg["modules"],
                "external": {}
            },
            "graph": {
                "internalCalls": [],
                "externalCalls": [],
                "resolvedCalls": []
            },
            "timestamp": cg["timestamp"],
            "sourcePath": cg["sourcePath"],
            "metadata": cg.get("metadata", {})
        }
        self.key_to_ns = {}
        self.key_to_super = {}
        self.counter = -1

    def encode(self, item):
        return urllib.parse.quote(item, safe="/().")

    def add_internal_calls(self):
        for src, dst in self.cg["graph"]["internalCalls"]:
            self.new_cg["graph"]["internalCalls"].append([str(src), str(dst), {}])

    def extract_counter(self):
        for mod in self.new_cg["modules"]["internal"].values():
            for key, value in mod["namespaces"].items():
                self.key_to_ns[int(key)] = self.encode(value["namespace"])
                self.counter = max(self.counter, int(key))

    def extract_superclasses(self):
        for key, superclasses in self.cg["cha"].items():
            scs = []
            # convert superClasses items to strings
            for item in superclasses:
                try:
                    mint = int(item)
                    scs.append(self.key_to_ns[mint])
                except ValueError:
                    scs.append(self.encode(item))
                    self.add_external(self.encode(item))
            self.key_to_super[int(key)] = scs

    def add_external(self, item):
        modname = item.split("/")[2]
        if not modname in self.new_cg["modules"]["external"]:
            self.new_cg["modules"]["external"][modname] = {
                "sourceFile": "",
                "namespaces": {}
            }

        # find out if the uri already exists
        found = False
        for k, v in self.new_cg["modules"]["external"][modname]["namespaces"].items():
            if v["namespace"] == item:
                cnt = int(k)
                found = True
                break

        if not found:
            self.counter += 1
            cnt = self.counter
            self.new_cg["modules"]["external"][modname]["namespaces"][str(cnt)] = {
                "namespace": item,
                "metadata": {}
            }

        return cnt

    def add_superclasses(self):
        for mod in self.new_cg["modules"]["internal"].values():
            for key, value in mod["namespaces"].items():
                if int(key) in self.key_to_super:
                    value["metadata"]["superClasses"] = self.key_to_super[int(key)]
                value["namespace"] = self.encode(value["namespace"])

    def add_external_calls(self):
        for src, dst in self.cg["graph"]["externalCalls"]:
            cnt = self.add_external(self.encode(dst))
            self.new_cg["graph"]["externalCalls"].append([str(src), str(cnt), {}])

    def convert(self):
        self.add_internal_calls()
        self.extract_counter()
        self.extract_superclasses()
        self.add_superclasses()
        self.add_external_calls()
        self.new_cg["nodes"] = self.counter + 1

    def output(self):
        return self.new_cg



class CallGraphGenerator:
    def __init__(self, out_topic, err_topic, source_dir, producer, release):
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.producer = producer
        self.release = release
        self.source_dir = Path(source_dir)

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

        # Root directory for tmp data
        self.out_root = Path("pycg-data")
        self.out_dir = self.out_root/self.product/self.version
        self.out_file = self.out_dir/'cg.json'
        self.downloads_dir = self.out_root/"downloads"
        self.untar_dir = self.out_root/"untar"

        # Where the source code will be stored
        self.source_path = self.source_dir/("sources/{}/{}/{}".format(
                    self.product[0], self.product, self.version))
        # Where the call graphs will be stored
        self.cg_path = self.source_dir/("callgraphs/{}/{}/{}".format(
                    self.product[0], self.product, self.version))

        self._create_dir(self.source_dir)
        self._create_dir(self.out_dir)

        # template for error messages
        self.error_msg = {
            'phase': '',
            'message': ''
        }

    def generate(self):
        try:
            comp = self._download()
            package = self._decompress(comp)
            self._copy_source(package)
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

    def _copy_source(self, pkg):
        try:
            if not self.source_path.exists():
                self.source_path.mkdir(parents=True)
            if os.path.isdir(pkg):
                dir_util.copy_tree(pkg, self.source_path.as_posix())
            else:
                fname = os.path.basename(pkg)
                shutil.copyfile(pkg, (self.source_path/fname).as_posix())
        except Exception as e:
            self._format_error('pkg-copy', str(e))
            raise CallGraphGeneratorError()

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

    def _convert_cg(self, cg):
        cgconverter = CGConverter(cg)
        cgconverter.convert()
        return cgconverter.output()

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
        cg["sourcePath"] = self.source_path.as_posix()

        # convert call graph to new format
        out_cg = self._convert_cg(cg)

        # store it
        self._store_cg(out_cg)

        output = dict(
                payload=out_cg,
                plugin_name=self.plugin_name,
                plugin_version=self.plugin_version,
                input=self.release,
                created_at=self._get_now_ts()
        )

        self.producer.send(self.out_topic, json.dumps(output))

    def _store_cg(self, out_cg):
        if not self.cg_path.exists():
            self.cg_path.mkdir(parents=True)

        with open((self.cg_path/"cg.json").as_posix(), "w+") as f:
            f.write(json.dumps(out_cg))

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
                    source_dir, bootstrap_servers, group, poll_interval):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.source_dir = source_dir
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

            generator = CallGraphGenerator(self.out_topic, self.err_topic,
                    self.source_dir, self.producer, release)
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
