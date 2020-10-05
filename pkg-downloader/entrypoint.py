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

class PackageDownloader:
    def __init__(self, out_topic, err_topic, source_dir, producer, release):
        self.out_topic = out_topic
        self.err_topic = err_topic
        self.producer = producer
        self.release = release
        self.source_dir = source_dir

        self.product = self.release['payload']['product']
        self.version = self.release['payload']['version']

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

    def download(self):
        try:
            comp = self._download()
            package = self._decompress(comp)
            source_path = self._copy_source(package)
            self._produce_source(source_path)
        except DownloadError:
            self._produce_error()
        finally:
            self._clean_dirs()

    def _format_error(self, phase, message):
        self.error_msg['phase'] = phase
        self.error_msg['message'] = message
        print (self.error_msg)

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

    def _produce_source(self, source_path):
        release = self.release
        release['payload']['sourcePath'] = source_path
        self.producer.send(self.out_topic, json.dumps(release))

    def _copy_source(self, pkg):
        source_path = os.path.join(self.source_dir,
                "sources/{}/{}/{}".format(
                    self.product[0], self.product, self.version))
        self._create_dir(source_path)
        try:
            distutils.dir_util.copy_tree(pkg, source_path)
        except as e:
            self.format_error('pkg-copy', str(e))
            raise DownloadError()

    def _create_dir(self, path):
        if not path.exists():
            path.mkdir(parents=True)

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
            raise DownloadError()

        items = list(self.downloads_dir.iterdir())
        if len(items) != 1:
            self._format_error(err_phase,\
                'Expecting a single downloaded item {}'.format(str(items)))
            raise DownloadError()

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
            raise DownloadError()

        try:
            out, err = self._execute(cmd)
        except Exception as e:
            self._format_error(err_phase, str(e))
            raise DownloadError()

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
            raise DownloadError()

        return items[0]

    def _clean_dirs(self):
        # clean up directories created
        if self.downloads_dir.exists():
            shutil.rmtree(self.downloads_dir.as_posix())
        if self.untar_dir.exists():
            shutil.rmtree(self.untar_dir.as_posix())
        if self.out_root.exists():
            shutil.rmtree(self.out_root.as_posix())

class DownloadError(Exception):
    pass

class PyPIDownloader:
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
                release['payload']['product']
            ))

            downloader = PackageDownloader(self.out_topic, self.err_topic, self.source_dir, self.producer, release)
            downloader.download()

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
        help="Directory where source code will be stored."
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
    source_dir = args.source_dir
    bootstrap_servers = args.bootstrap_servers
    group = args.group
    sleep_time = args.sleep_time
    poll_interval = args.poll_interval

    consumer = PyPIDownloader(
        in_topic, out_topic, err_topic,\
        source_dir, bootstrap_servers, group, poll_interval)

    while True:
        consumer.consume()
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
