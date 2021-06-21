FROM ubuntu:latest

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq -y update && apt-get -qq -y install python3-pip libpq-dev python3-icu git curl unzip csvkit rclone
COPY . /opt/ftg
RUN pip3 install -q -e /opt/ftg
# RUN curl https://rclone.org/install.sh | bash
WORKDIR /opt/ftg
CMD /bin/bash
