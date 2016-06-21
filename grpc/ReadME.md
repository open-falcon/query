# OWL Grpc Server
-----------
what is Grpc? Please refer [grpc](http://www.grpc.io/)

## 安裝需要的package
-----------
``` $sh
cd grpc
cp -r sgolang.org $GOPATH<br>
go get google.golang.org/grpc
```
ps. 有grpc的[issue](google.golang.org/grpc)說china或許抓不到

## 如何compile proto檔
----------
* 需要先安裝系統必須的package [以下是for ubuntu]
``` $sh
#機器上可能缺少一些compile protoc 必須的package, 需要先預先安裝
apt-get install unzip dh-autoreconf
git clone https://github.com/google/protobuf
cd protobuf
./autogen.sh
./configure --perfix=/opt/protobuf
make
make install
export PATH=/opt/protobuf/bin:$GOPATH/bin:$PATH
go get google.golang.org/grpc
git clone https://github.com/golang/protobuf
cd protobuf
```
* compile
``` $sh
./gen.sh proto/owlquery/grafana.proto
```
* 之後跟著query整個project run `go build  就可以產生可執行檔
