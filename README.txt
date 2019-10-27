Special instructions for compiling/running the code should be included in this file.
Install dependencies:
    go get gopkg.in/mgo.v2
    go get github.com/DistributedClocks/GoVector
Run MasterServer
    go run MasterServer.go -c ./server/config.json
    (Change the server address and port by modifying the config.json file)
Run ResourceNode
    go run resourceNode.go [server ip:port] [private key file number]
    (ex. go run resourceNode.go 0.0.0.0:12345 1)
Run Client
    go run client.go [server ip:port]
    (ex. go run client.go 0.0.0.0:12345)