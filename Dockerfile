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
RUN apt-get install -y build-essential
RUN wget -qO- "https://cmake.org/files/v3.17/cmake-3.17.0-Linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local
RUN apt-get install -y git
RUN apt-get install -y maven
RUN curl -L -o cryptominisat-5.8.0.tar.gz https://github.com/msoos/cryptominisat/archive/refs/tags/5.8.0.tar.gz \
 && tar -xzf cryptominisat-5.8.0.tar.gz \
 && rm cryptominisat-5.8.0.tar.gz \
 && cd cryptominisat-5.8.0 \
 && mkdir build \
 && cd build \
 && cmake .. \
 && make -j4 \
 && sudo make install
RUN apt-get install -y libmetis-dev
RUN git clone https://github.com/Francisco-Rola/separtitioning.git \
 && cd separtitioning \
 && cp resources/* /usr/local/bin \
 # Run Maven build
 && mvn clean install
