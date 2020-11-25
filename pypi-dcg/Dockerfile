#vim:set ft=dockerfile:
FROM debian:stable

# install python3
RUN apt -yqq update && apt install -yqq\
                        python3\
                        python3-pip\
                        git

# create dirs
RUN mkdir /cggen
RUN git clone https://github.com/vitsalis/pycallgraph2 /pycallgraph2
RUN cd /pycallgraph2; python3 setup.py install

# move files
COPY ./entrypoint.py /cggen/entrypoint.py

WORKDIR /cggen
ENTRYPOINT ["python3", "entrypoint.py"]
