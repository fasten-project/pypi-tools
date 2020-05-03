import os
import json
import time
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

        self.out_root = Path("callgraphs")
        self.out_dir = self.out_root/self.product/self.version
        if not self.out_dir.exists():
            self.out_dir.mkdir(parents=True)

        self.out_file = self.out_dir/'cg.json'

        self.downloads_dir = Path("downloads")
        self.untar_dir = Path("untar")

        self.error_msg = {
            'product': self.product,
            'version': self.version,
            'datetime': str(datetime.datetime.now()),
            'phase': '',
            'message': ''
        }

    def generate(self):
        try:
            comp = self._download()
            package = self._decompress(comp)
            cg_path = self._generate_callgraph(package)
            self._produce_callgraph(cg_path)
        except CallGraphGeneratorError:
            self._produce_error()
        finally:
            self._clean_dirs()

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
                'Did not download only one item {}'.format(str(items)))
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
            self._format_error(err_phase,\
                'More than one items untarred {}'.format(str(items)))
            raise CallGraphGeneratorError()

        return items[0]

    def _generate_callgraph(self, package_path):
        # call pycg using `package`
        files_list = self._get_python_files(package_path)
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

        try:
            out, err = self._execute(cmd)
        except Exception as e:
            self._format_error('generation', str(e))
            raise CallGraphGeneratorError()

        return self.out_file

    def _get_python_files(self, package):
        return [x.resolve().as_posix().strip() for x in package.glob("**/*.py")]

    def _produce_callgraph(self, cg_path):
        # produce call graph to kafka topic
        if not cg_path.exists():
            self._format_error('producer',\
                'Call graph path does not exist {}'.format(cg_path.as_posix()))
            raise CallGraphGeneratorError()

        with open(cg_path.as_posix(), "r") as f:
            cg = json.load(f)

        # PyCG must produce a call graph with the key "depset"
        # The key "depset" must have the same format that comes from the input topic
        # if we have a requires_dist from the input topic
        # replace the one that pycg found from the requirements.txt file
        if len(self.requires_dist):
            cg["depset"] = self.requires_dist

        self.producer.send(self.out_topic, json.dumps(cg))

    def _produce_error(self):
        # produce error to kafka topic
        self.producer.send(self.err_topic, json.dumps(self.error_msg))

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

    def _create_dir(self, path):
        if not path.exists():
            path.mkdir()

class CallGraphGeneratorError(Exception):
    pass

class PyPIConsumer:
    def __init__(self, in_topic, out_topic, err_topic,\
                    bootstrap_servers, group):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.group = group

    def consume(self):
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

        for message in self.consumer:
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

    consumer = PyPIConsumer(
        in_topic, out_topic, err_topic,\
        bootstrap_servers, group)

    while True:
        consumer.consume()
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
