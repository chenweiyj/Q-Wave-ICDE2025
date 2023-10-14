#!/bin/bash

start=1
end=$(($start-1+100))

for i in `seq $start $end`; do
  echo -n "${i}  "
  docker run --name chain-shard1-$i --net=host -d chain -d /ip4/127.0.0.1/tcp/33033/p2p/QmXujgrDSSqJVCkHsrWYEfp6dTetY7TkyNPbVmZicsaHaH -sp $((30333+$i))
done

