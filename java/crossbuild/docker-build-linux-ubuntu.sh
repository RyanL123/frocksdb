#!/usr/bin/env bash
set -e

mkdir -p /rocksdb-local-build
rm -rf /rocksdb-local-build/*
cp -r /rocksdb-host/* /rocksdb-local-build
cd /rocksdb-local-build

PORTABLE=1 USE_RTTI=1 make -j$(nproc) rocksdbjava

cp java/target/librocksdbjni-linux*.so \
   java/target/rocksdbjni-*-linux*.jar \
   java/target/rocksdbjni-*-linux*.jar.sha1 \
   /rocksdb-java-target/    