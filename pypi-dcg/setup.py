#
# Copyright (c) 2018-2021 FASTEN.
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

from setuptools import setup, find_packages

def get_long_desc():
    with open("README.md", "r") as readme:
        desc = readme.read()

    return desc

def setup_package():
    setup(
        name='pydyncg',
        version='0.0.1',
        description='Dynamic Python Call Graphs',
        long_description=get_long_desc(),
        long_description_content_type="text/markdown",
        license='Apache Software License',
        packages=find_packages(),
        install_requires=[],
        entry_points={
            'console_scripts': [
                'pydyncg=pydyncg.pydyncg:main',
            ],
        },
        classifiers=[
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3'
        ],
        author='Vitalis Salis',
        author_email='vitsalis@gmail.com'
    )

if __name__ == '__main__':
    setup_package()
