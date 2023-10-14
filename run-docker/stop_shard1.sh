#!/bin/bash

for i in `seq 0 500`; do
  docker stop chain-shard1-$i
  docker rm chain-shard1-$i
done
