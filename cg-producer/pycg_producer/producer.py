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
import shutil
import datetime
import subprocess as sp
import cmdbench

from pathlib import Path
from distutils import dir_util


class CallGraphGenerator:
    def __init__(self, source_dir, release):
        self.release = release
        self.source_dir = Path(source_dir)
        self.output = { "Status":"",
                        "Output":""}
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
            return self.output

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
            for file in items:
                if (file.startswith(self.product) | file.startswith(self.product.replace("-", "_"))):
                    return file
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
        items_temp = list(self.untar_dir.iterdir())
        items = [val for val in items_temp if ((not str(val).endswith(".data")) & (not str(val).endswith(".pth")) & (not str(val).endswith("/tests"))& (not str(val).endswith("/docs")))]
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
                pkg_path = pkg.as_posix()
                # if the package path contains an init file
                # then the PyCG will generate a call graph for its parent
                if (pkg/"__init__.py").exists():
                    pkg_path = pkg.parent.as_posix()

                dir_util.copy_tree(pkg_path, self.source_path.as_posix())
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

        result = self._execute_with_benchmark(cmd)

        if result["process"]["stderr_data"]:
            self._format_error('generation', result["process"]["stderr_data"].strip())
            raise CallGraphGeneratorError()

        if not self.out_file.exists():
            self._format_error('generation', result["process"]["stderr_data"].strip())
            raise CallGraphGeneratorError()
        self.elapsed = result["process"]["execution_time"]
        self.max_rss = round(result["memory"]["max"]/1000)
        return self.out_file

    def _get_python_files(self, package):
        return [x.as_posix().strip() for x in package.glob("**/*.py")]

    def _get_lines_of_code(self, files_list):
        res = 0
        for fname in files_list:
            with open(fname) as f:
                try:
                    res += sum(1 for l in f if l.rstrip())
                except UnicodeDecodeError as e:
                    continue

        return res


    def _execute_with_benchmark(self, cmd):
        benchmark_results = cmdbench.benchmark_command(' '.join(map(str, cmd)))
        first_iteration_result = benchmark_results.get_first_iteration()
        return first_iteration_result

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
                    'Call graph is not JSON formatted {}. Contents {}'.format(cg_path.as_posix(), f.read()))
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

        # store it
        self._store_cg(cg)

        output = dict(
                payload=cg,
                plugin_name=self.plugin_name,
                plugin_version=self.plugin_version,
                input=self.release,
                created_at=self._get_now_ts()
        )

        self.output["Status"] = "Success"
        self.output["Output"] = output

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
        # produce error
        output = dict(
            plugin_name=self.plugin_name,
            plugin_version=self.plugin_version,
            input=self.release,
            created_at=self._get_now_ts(),
            err=self.error_msg
        )
        self.output["Status"] = "Fail"
        self.output["Output"] = output

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
