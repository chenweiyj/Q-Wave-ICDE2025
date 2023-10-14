# 构建
## 打印计时代码部分的日志
> go build -tags debug

## 不打印计时代码部分的日志
> go build

# 运行
## 启动super node
super node用来做消息转发

> ./chain -sp 30333 -c 5000 -times 1000 &> bbb.log
-sp 监听端口
-c 实验客户端数
-times 执行实验次数

## 启动client node
> ./chain -d /ip4/127.0.0.1/tcp/33033/p2p/QmbhRSBvxTH9j3abgW2fPCpifH5aTegFdD2XdUJB1xdHHJ -c 5000 &> aaa.log

-d super node连接地址，由super node启动后从其日志中获取
-c 实验客户端数

# 结果
执行times次的时间会记录在records.csv中，但会排除掉超时的部分。如执行times=1000次，13次超时，则records.csv中的记录为987条。
