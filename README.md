# p2pproxy

将本地服务代理到目标网络

代理功能是基于gevent开发，使用前先安装gevent包

比如：代理本地的8080端口到172.30.0.x网络 80端口

# client
p2pproxy -c -p2p=172.30.0.x:11110 -server=127.0.0.1:8080
 
# server 
p2pproxy -s -p2p=:11110  -server=:80

