FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true
RUN  apt-get update  -y \
  && apt-get install -y software-properties-common \
  && add-apt-repository ppa:openjdk-r/ppa \
  && apt-get install -y openjdk-8-jdk \
  && apt-get -y install libgmpxx4ldbl && apt-get -y install libgmp-dev \
  && apt-get -y install build-essential cmake \
  && apt-get -y install zlib1g-dev \
  && apt-get -y install libboost-all-dev \
  && apt-get -y install libm4ri-dev \
  && rm -rf /var/lib/apt/lists/*
# java
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN apt update && apt install -y curl avahi-daemon wget sshpass sudo locales locales-all ssh vim expect libfontconfig1 libgl1-mesa-glx libasound2
RUN apt-get install -y build-essential cmake
RUN apt-get install -y git
RUN apt-get install -y maven
RUN git clone https://github.com/usi-verification-and-security/opensmt.git \
 && cd opensmt \
 && mkdir build \
 && cd build \
 && cmake .. \
 && make \
 && make install
RUN git clone https://github.com/msoos/cryptominisat \
 && cd cryptominisat \	
 && mkdir build && cd build \
 && cmake .. \
 && make \
 && sudo make install \
 && cd ../..
RUN git clone https://github.com/Francisco-Rola/separtitioning.git \
 && cd separtitioning/thesis \
 && mvn clean install

