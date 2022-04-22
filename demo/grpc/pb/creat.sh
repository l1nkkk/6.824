#!/bin/bash
script_dir=$(cd $(dirname $0);pwd)
#echo $script_dir


protoc -I $script_dir/ $script_dir/hello.proto --go_out=plugins=grpc:$script_dir

#protoc --go_out=../ --go_opt=paths=source_relative \
#    --go-grpc_out=../ --go-grpc_opt=paths=source_relative \
#    ./hello.proto