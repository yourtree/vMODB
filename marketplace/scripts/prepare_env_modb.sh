#!/usr/bin/env bash

# benchmark driver
sudo apt-get update && \
  sudo apt-get install -y dotnet-sdk-7.0

# control number of cpus in experiments
sudo apt install cpulimit

# install jdk 21
sudo apt install -y openjdk-21-jdk

# remember to set java home manually or run below
# JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >>~/.bashrc