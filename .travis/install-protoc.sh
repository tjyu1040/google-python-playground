#!/usr/bin/env bash
# Script to install protocol buffer compiler. Works for Linux or Mac OS X.

protoc_version=3.3.0

platform=$(uname -s)
if [ "${platform}" == "Darwin" ]; then
    platform=osx
elif [ "${platform}" == "Linux" ]; then
    platform=linux
else
    echo "ERROR: Failed to determine OS platform."
    exit 1
fi

release_binary=protoc-${protoc_version}-${platform}-x86_64.zip

echo ${release_binary}
#curl -OL https://github.com/google/protobuf/releases/download/v${protoc_version}/${release_binary}
#unzip ${release_binary} -d $HOME/protobuf
