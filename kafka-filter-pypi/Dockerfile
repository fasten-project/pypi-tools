#vim:set ft=dockerfile:
FROM debian:stable

# install python3
RUN apt -yqq update && apt install -yqq\
                        python3\
                        python3-pip

RUN pip3 install kafka-python python-dateutil

# create dirs
RUN mkdir /filter
# move files
COPY ./entrypoint.py /filter/entrypoint.py

WORKDIR /filter
ENTRYPOINT ["python3", "entrypoint.py"]
