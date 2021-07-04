import os
import ast
import glob
import json
import time
import kafka
import shutil
import random
import argparse
import datetime
import unittest
import subprocess

from distutils import dir_util
from kafka import KafkaConsumer, KafkaProducer

ROUND = 3
COVERAGE_PER = 1
METHOD_PYTEST = "METHOD_PYTEST"
METHOD_UNITTEST = "METHOD_UNITTEST"
LOCAL_MOUNT = "/local"

class FunctionCounter(ast.NodeVisitor):
    def __init__(self):
        self.func_count = 0
        super().__init__()

    def visit_FunctionDef(self, node):
        self.func_count += 1
        for stmt in node.body:
            self.visit(stmt)

    def get_func_count(self):
        return self.func_count

class StatsCollector:
    def __init__(self, out_topic, source_dir, producer, release, copy_sources, collect_method):
        self.out_topic = out_topic
        self.source_dir = source_dir
        self.producer = producer
        self.release = release
        self.copy_sources = copy_sources
        self.method = collect_method

        self.path = self._get_path()

    def _get_path(self):
        product = self.release['product']
        version = self.release['version']
        pkg_dir = os.path.join(self.release['product'][0], self.release['product'], self.release['version'])
        mounted = os.path.join(self.source_dir, "sources", pkg_dir)
        if not os.path.exists(mounted):
            print ("Source directory does not exist: ", mounted)
            return None

        if self.copy_sources:
            local = os.path.join(LOCAL_MOUNT, pkg_dir)
            shutil.copytree(mounted, local)
            return local
        else:
            return mounted

    def _get_environment(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = self.path
        return env

    def _analyze_pytest_lines(self, lines):
        data = {}
        failed = False
        failed_count = 0
        for line in lines:
            if not line.strip():
                failed = True
                continue

            if failed:
                if line.startswith(b'ERROR'):
                    failed_count += 1
            else:
                splitted = line.strip().split(b'::')
                if len(splitted) == 3:
                    fpath, _, tst_name = splitted
                elif len(splitted) == 2:
                    fpath, tst_name = splitted
                else:
                    continue

                if not data.get(fpath, None):
                    data[fpath] = []

                data[fpath].append(tst_name)

        test_files_count = 0
        tests_count = 0
        for tests in data.values():
            test_files_count += 1
            tests_count += len(tests)

        return test_files_count, tests_count, failed_count

    def collect_pytest_stats(self):
        env = self._get_environment()

        cmd = ["pytest", "--collect-only", "--quiet", self.path]
        pp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        return self._analyze_pytest_lines(pp.stdout.readlines())

    def collect_unittest_stats(self):
        def count_tests(tests):
            test_files_count = 0
            tests_count = 0
            tests_failed = 0
            for test in tests:
                cnt = 0
                for t in test:
                    if isinstance(t, unittest.loader._FailedTest):
                        tests_failed += 1
                        continue
                    cnt += 1
                if cnt > 0:
                    test_files_count += 1

                tests_count += cnt

            return test_files_count, tests_count, tests_failed

        def discover(pattern):
            try:
                return unittest.TestLoader().discover(self.path, pattern=pattern, top_level_dir=self.path)
            except Exception as e:
                return []

        tests = discover("*_test.py")
        fcount, tcount, failedcount = count_tests(tests)
        if fcount == 0:
            tests = discover("test*")
            fcount, tcount, failedcount = count_tests(tests)

        return fcount, tcount, failedcount

    def collect_source_files(self):
        return glob.glob(self.path + "/**/*.py", recursive=True)

    def collect_functions(self, source_files):
        func_count = 0
        for srcf in source_files:
            with open(srcf, "r") as f:
                parsed = ast.parse(f.read())
            counter = FunctionCounter()
            counter.visit(parsed)
            func_count += counter.get_func_count()

        return func_count

    def collect_coverage(self):
        env = self._get_environment()

        cmd = ["pytest", "--collect-only", "--quiet", "--cov="+self.path, self.path]
        pp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        cov_found = False
        for line in pp.stdout.readlines():
            decoded = line.decode('ascii').strip()
            if "coverage" in decoded:
                cov_found = True
            if cov_found and decoded.startswith("TOTAL"):
                coverage = decoded.split(" ")[-1][:-1]
                try:
                    return int(coverage) / 100
                except Exception:
                    return "-"
        return "-"

    def collect(self):
        def get_ratios(tc, tfc, fc, sfc):
            unit_func_ratio = "-"
            if fc != 0:
                unit_func_ratio = round(tc/fc, ROUND)
            test_source_ratio = "-"
            if sfc != 0:
                test_source_ratio = round(tfc/sfc, ROUND)
            return unit_func_ratio, test_source_ratio

        if not self.path:
            return

        start = time.time()

        source_files = self.collect_source_files()
        source_files_count = len(source_files)
        functions_count = self.collect_functions(source_files)

        test_files_count_pytest, tests_count_pytest, tests_failed_pytest = self.collect_pytest_stats()
        test_files_count_unittest, tests_count_unittest, tests_failed_unittest = self.collect_unittest_stats()

        unit_func_ratio_pytest, test_source_ratio_pytest = get_ratios(tests_count_pytest,
                    test_files_count_pytest, functions_count, source_files_count)
        unit_func_ratio_unittest, test_source_ratio_unittest = get_ratios(tests_count_unittest,
                    test_files_count_unittest, functions_count, source_files_count)

        coverage = "-"
        if test_files_count_pytest > 0 and random.random() < 1/COVERAGE_PER:
            coverage = self.collect_coverage()

        end = time.time()

        self.produce({
            "product": self.release["product"],
            "version": self.release["version"],
            "path": self.path,
            "sourceFiles": source_files_count,
            "functions": functions_count,
            "pytest": {
                "unitTests": tests_count_pytest,
                "testFiles": test_files_count_pytest,
                "unitTestsToFunctionsRatio": unit_func_ratio_pytest,
                "testToSourceRatio": test_source_ratio_pytest,
                "failed": tests_failed_pytest
            },
            "unittest": {
                "unitTests": tests_count_unittest,
                "testFiles": test_files_count_unittest,
                "unitTestsToFunctionsRatio": unit_func_ratio_unittest,
                "testToSourceRatio": test_source_ratio_unittest,
                "failed": tests_failed_unittest
            },
            "coverage": coverage,
            "time": round(end-start, ROUND)
        })

        self.delete_local()

    def produce(self, data):
        self.producer.send(self.out_topic, json.dumps(data))

    def delete_local(self):
        if self.copy_sources:
            shutil.rmtree(self.path)

class Consumer:
    def __init__(self, in_topic, out_topic, source_dir, bootstrap_servers, group, poll_interval, copy_sources):
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.source_dir = source_dir
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.group = group
        self.poll_interval = poll_interval
        self.copy_sources = copy_sources

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
            print ("{}: Consuming {}-{}".format(
                datetime.datetime.now(),
                release['product'], release['version']
            ))

            collector = StatsCollector(self.out_topic, self.source_dir, self.producer, release, self.copy_sources, METHOD_PYTEST)
            collector.collect()

def get_parser():
    parser = argparse.ArgumentParser(
        """
        Consume packages from a Kafka topic and generate statistics
        regarding their tests into a Kafka topic.
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
    parser.add_argument(
        '--copy-sources',
        action="store_true",
        help="Copy source directories into local directories",
        default=False)
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    in_topic = args.in_topic
    out_topic = args.out_topic
    bootstrap_servers = args.bootstrap_servers
    group = args.group
    sleep_time = args.sleep_time
    source_dir = args.source_dir
    poll_interval = args.poll_interval
    copy_sources = args.copy_sources

    consumer = Consumer(
        in_topic, out_topic, source_dir,
        bootstrap_servers, group, poll_interval, copy_sources)

    while True:
        consumer.consume()
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
