#!/bin/bash

docker run --rm --name chain-master --net=host chain -sp 30333

# docker run --rm --name chain-shard1-0 --net=host chain -d /ip4/127.0.0.1/tcp/30333/p2p/QmcnAcTJAW4xjJonBCsaLBTzpVLhVotZFnN9zsvnKyRjUf

