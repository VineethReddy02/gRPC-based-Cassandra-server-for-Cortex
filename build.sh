#!/bin/bash

cd grpc/ && protoc --go_out=plugins=grpc:. *.proto && cd ..
go build -o bin/cortex-cassandra-store cortex-cassandra-store/cassandra
