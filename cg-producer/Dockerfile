#vim:set ft=dockerfile:
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
COPY ./pycg/ /pycg
COPY ./entrypoint.py /cggen/entrypoint.py

# install pycg and make wrapper executable
RUN cd /pycg && python3 setup.py install

WORKDIR /cggen
ENTRYPOINT ["python3", "entrypoint.py"]
