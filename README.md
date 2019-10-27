# Distributed K-means 
Based on the MapReduced paradigm and implemented in Go
## Setup
- Install [Go](https://golang.org/)
- Install dependencies:
      `go get gopkg.in/mgo.v2`
      `go get github.com/DistributedClocks/GoVector`
## Node Types
There are three types of nodes in the system
1. ***MasterServer***  
   Roles:
   * Handle the joining and leaving of ResourceNodes, detect ResourceNodes failures through heartbeat
   * Accept requests for data classificaiton, assign tasks to ResourceNodes, and notify the client once classification has completed  
   
   Run:  
   go run MasterServer.go -c ./server/config.json  
   (change the server address and port by modifying the config.json file)  
2. ***ResourceNode***  
    Roles:
    * Join the network with their own training data, which will contribute to the training of the global K-means model
    * While connected, work on map/reduce tasks following MasterServer's instructions  
    
    Run:  
    go run resourceNode.go [server ip:port] [private key file number]  
    (ex. `go run resourceNode.go 0.0.0.0:12345 1` will start a ResourceNode with private key stored in `privkeys/priv1`  
3.  ***Client***  
    Roles:
    * Send request to Master to obtain classification results for its data  
    * Communicate with Master through API defined in clientlib.go  
    Run:  
    go run client.go [server ip:port]  
    (ex. go run client.go 0.0.0.0:12345)  
    
## Design

[A review of K-means algorithm](https://en.wikipedia.org/wiki/K-means_clustering) 
### Training  
  * At the beginning, MasterServer initializes k random means  
  * Map
    - MasterServer sends the means to ResourceNodes
    - Each ResouceNodes computes the nearest mean for every entry in its local dataset, and group them based on their closet means
    - For each group, ResourceNode computes the sum of distances to the group mean and count the number of entries in the group  
 * Reduce
    - MasterServer creates k tasks and assigns the tasks to ResourceNodes
    - ResourceNode received task i will request sum and count for the ith mean from all other ResourceNodes, aggregate the sums and counts and computes the ith new mean by dividing the total sum of distances by the total count
    - MasterServer collects the new means and starts the next iteration, going back to map phase again
### Classifying
  * Client transfers data for classification to CosmoDB
  * Client sends request to MasterServer, providing database credentials and a number N, which is the number of data entry to be classified
  * MasterServer creates N tasks and splits them among ResourceNodes
  * Map
    - ResourceNode retrieves the assigned data from database in batch, find the closest mean for each data entry and write the result back to the database

## Features
* Stable K-means.  Since K-means is sensative to initialization, train 50 K-means models, each with some random initialization, and picks the model with the lowest total sum of distances
* Load balancing.  MasterServer records the performance (i.e., time to task completion) of each ResourceNode, assigning more tasks to faster node.
* Backup tasks.  If a ResourceNode fails to reponse in a reasonable time frame, MasterServer reassigns its tasks to other ResourceNode who have already complete their tasks.   
* Visualization.  Clients can view the classification results by opening `frontend/index.html`.  All data entries will be plotted and entries belong to different cluster will have different colors.       