# Introduction

Query面向终端用户，收到查询请求后，根据一致性哈希算法，会去相应的Graph(或DRRS)里面，查询不同metric的数据，汇总后统一返回给用户。

注：DRRS是京东金融集团杭州研发团队的同事开发的一个轻量级的分布式环形数据服务组件，用于监控数据的持久化和绘图。该组件作用于graph组件类似，并且能够在保证绘图效率的前提下实现秒级扩容。参考https://github.com/jdjr/drrs

## 查询历史数据
查询过去一段时间内的历史数据，使用接口 `HTTP POST /graph/history`。该接口不能查询最新上报的两个数据点。一个python例子，如下

```python
#!-*- coding:utf8 -*-

import requests
import time
import json

end = int(time.time())
start = end - 3600  #查询过去一小时的数据

d = {
        "start": start,
        "end": end,
        "cf": "AVERAGE",
        "endpoint_counters": [
            {
                "endpoint": "host1",
                "counter": "cpu.idle",
            },
            {
                "endpoint": "host1",
                "counter": "load.1min",
            },
        ],
}

url = "http://127.0.0.1:9966/graph/history"
r = requests.post(url, data=json.dumps(d))
print r.text

```

其中cf的值可以为：AVERAGE、MAX、MIN ，具体可以参考RRDtool的相关概念

## 查询最新上报的数据
查询最新上报的一个数据点，使用接口`HTTP POST /graph/last`。一个bash的例子，如下

```bash
#!/bin/bash
if [ $# != 2 ];then
    printf "format:./last \"endpoint\" \"counter\"\n"
    exit 1
fi

# args
endpoint=$1
counter=$2

# form request body
req="[{\"endpoint\":\"$endpoint\", \"counter\":\"$counter\"}]"

# request 
url="http://127.0.0.1:9966/graph/last"
curl -s -X POST -d "$req" "$url" | python -m json.tool

```

## 源码编译
注意: 请首先更新common模块

```bash
# download source
cd $GOPATH/src/github.com/open-falcon
git clone https://github.com/open-falcon/query.git # or use git pull to update query

# update dependencies: open-falcon/common, toolkits/consistent, toolkits/pool
cd query
go get ./...

# compile and pack
./control build
./control pack
```
最后一步会pack出一个tar.gz的安装包，拿着这个包去部署服务即可。你也可以在[这里](https://github.com/open-falcon/query/releases)，下载最新发布的代码。

## 服务部署
服务部署，包括配置修改、启动服务、检验服务、停止服务等。这之前，需要将安装包解压到服务的部署目录下。

```bash
# 修改配置, 配置项含义见下文, 注意graph集群的配置
mv cfg.example.json cfg.json
vim cfg.json
# 注意: 从v1.4.0开始, 我们把graph列表的配置信息，移动到了cfg.json中，不再需要graph_backends.txt

# 启动服务
./control start

# 校验服务,这里假定服务开启了9966的http监听端口。检验结果为ok表明服务正常启动。
curl -s "127.0.0.1:9966/health"

...
# 停止服务
./control stop

```
服务启动后，可以通过日志查看服务的运行状态，日志文件地址为./var/app.log。可以通过`./test/debug`，查看服务的内部状态数据。可以通过scripts下的`query last`等脚本，进行数据查询。

## 配置文件格式说明
注意: 配置文件格式有更新; 请确保 `graph.replicas`和`graph.cluster` 的内容与transfer的配置**完全一致**

```bash
{
    "debug": "false",   // 是否开启debug日志
    "http": {
        "enable":  true,           // 是否开启http.server
        "listen":   "0.0.0.0:9966" // http.server监听地址&端口
    },
    "graph": {
        "connTimeout": 1000, // 单位是毫秒，与后端graph建立连接的超时时间，可以根据网络质量微调，建议保持默认
        "callTimeout": 5000, // 单位是毫秒，从后端graph读取数据的超时时间，可以根据网络质量微调，建议保持默认
        "maxConns": 32,      // 连接池相关配置，最大连接数，建议保持默认
        "maxIdle": 32,       // 连接池相关配置，最大空闲连接数，建议保持默认
        "replicas": 500,     // 这是一致性hash算法需要的节点副本数量，应该与transfer配置保持一致
        "cluster": {         // 后端的graph列表，应该与transfer配置保持一致；不支持一条记录中配置两个地址
            "graph-00": "test.hostname01:6070",
            "graph-01": "test.hostname02:6070"
        }
    },
	"drrs":{                       //启用此功能前请确保DRRS已被正确安装配置，不能与graph同时使用
		"enabled": false,          //true/false, 表示是否开启向DRRS发送数据 #不能和graph的enable同时为true
		"useZk": false,            //是否配置了zookeeper，若DRRS配置了多台master节点并且配置了zk，则配置为true
		"dest" : "127.0.0.1:12300",//DRRS中master节点的地址，若没有配置zk，则这里需要配置master节点的地址，格式为ip:port
		"replicas": 500,           //这是一致性hash算法需要的节点副本数量，建议不要变更，保持默认即可
		"maxIdle" : 32,            //连接池相关配置，最大空闲连接数，建议保持默认
		"zk": {                    //zookeeper的相关配置信息，若useZk设置为true，则需要配置以下信息
			"ip" : "10.9.0.130",   //zk的ip地址，zk的端口需要保持默认的2181
			"addr": "/drrs_master",//zk中DRRS配置信息的存放位置
			"timeout" : 10         //zk的超时时间
		}
	}
}
```

## 补充说明
部署完成query组件后，请修改dashboard组件的配置、使其能够正确寻址到query组件。请确保query组件的graph列表 与 transfer的配置 一致。

若配置DRRS的enable属性为true，请确保DRRS已被正确的安装部署。DRRS安装部署请参考官方github：[https://github.com/jdjr/drrs](https://github.com/jdjr/drrs)

