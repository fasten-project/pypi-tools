#vim:set ft=dockerfile:
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
FROM debian:stable

# install python3
RUN apt -yqq update && apt install -yqq\
                        python3\
                        python3-pip\
                        unzip\
			time

RUN pip3 install kafka-python

# create dirs
RUN mkdir /cggen

# move files
COPY ./PyCG/ /pycg
COPY ./entrypoint.py /cggen/entrypoint.py
COPY ./pycg_producer /cggen/pycg_producer


# install pycg and make wrapper executable
RUN cd /pycg && python3 setup.py install

WORKDIR /cggen
ENTRYPOINT ["python3", "entrypoint.py"]
