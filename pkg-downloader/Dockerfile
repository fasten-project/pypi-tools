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
RUN mkdir /sources

# move files
COPY ./entrypoint.py /cggen/entrypoint.py

WORKDIR /cggen
ENTRYPOINT ["python3", "entrypoint.py"]
