#!/bin/bash

COUNT=100

[ ! -d "tmp" ] && mkdir tmp
for i in `seq 0 $COUNT`; do head -c 1K </dev/urandom >tmp/randfile$i; done
for i in `seq 0 $COUNT`; do aws --endpoint-url http://localstack:4566 s3 sync tmp/ s3://ingress; done
for i in `seq 0 $COUNT`; do rm -r tmp/randfile$i; done
