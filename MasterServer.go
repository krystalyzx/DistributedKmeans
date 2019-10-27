package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"./util"
	"crypto/x509"
	"math"
	"net/http"

	"github.com/DistributedClocks/GoVector/govec"
)

// Errors that the server could return.
type UnknownKeyError error

type KeyAlreadyRegisteredError string

func (e KeyAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("BlockArt server: key already registered [%s]", string(e))
}

type AddressAlreadyRegisteredError string

func (e AddressAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("BlockArt server: address already registered [%s]", string(e))
}

// Settings for an instance of the BlockArt project/network.

type RServer int

type ResourceNode struct {
	Address         net.Addr
	RecentHeartbeat int64
	conn            *rpc.Client
	efficiency      float64
	pubkey          string
}

type Config struct {
	ResourceNodeSettings util.ResourceNodeSettings `json:"settings"`
	RpcIpPort            string                    `json:"rpc-ip-port"`
}

type AllResourceNodes struct {
	all map[string]*ResourceNode
}

type TaskStatuses struct {
	failedTaskQueue map[int]bool // bool assigned or not assigned
	workerTaskMap map[string]map[int]bool
	allTasks map[int]bool
	taskStatusMutex  *sync.Mutex
	currentPhase int   // 0 -> idle, 1 -> trainmap, 2->trainReduce, 3->classify
	startBackUpTask bool

}

type Model struct {
	K                int
	KMeans           map[int][]float64
	KMeansErrorMutex *sync.Mutex
	KMeansError      float64
}

var (
	unknownKeyError UnknownKeyError = errors.New("BlockArt server: unknown key")
	config          Config
	errLog          *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog          *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	// Miners in the system.

	allResourceNodes AllResourceNodes = AllResourceNodes{all: make(map[string]*ResourceNode)}
	//resourceNodeWorkTime = make(map[string]int)
	splitWorkUsedinClassify                   = false
	authorizedKeys            map[string]bool = make(map[string]bool)
	globalModel                               = Model{K: 0, KMeans: make(map[int][]float64), KMeansErrorMutex: &sync.Mutex{}, KMeansError: float64(0)}
	clientMutex               *sync.Mutex     = &sync.Mutex{}
	mapTasKMutex              *sync.Mutex     = &sync.Mutex{}
	mapTaskChan               chan TaskStatus = make(chan TaskStatus)
	reduceTaskChan            chan TaskStatus = make(chan TaskStatus)
	mapTaskSuccessNodeAddress []net.Addr
	resultSvg                 string

	resultX      map[int][]float64 = make(map[int][]float64)
	resulty      map[int]int       = make(map[int]int)
	taskStatuses = TaskStatuses{
		make(map[int]bool),
		make(map[string]map[int]bool),
		make(map[int]bool),
		&sync.Mutex{},
		0,
		false}

	testData     map[int][]float64
	username     string
	password     string
	needToTrain  bool
	MasterLogger = govec.InitGoVector("Master", "MasterLogFile")
	collection = "xhat"
)

func readConfigOrDie(path string) {
	file, err := os.Open(path)
	handleErrorFatal("config file", err)

	buffer, err := ioutil.ReadAll(file)
	handleErrorFatal("read config", err)

	err = json.Unmarshal(buffer, &config)
	handleErrorFatal("parse config", err)

}

// Parses args, setups up RPC server.
func main() {

	//MasterLogger.DisableLogging()
	gob.Register(&net.TCPAddr{})
	gob.Register(&elliptic.CurveParams{})

	path := flag.String("c", "", "Path to the JSON config")
	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	readConfigOrDie(*path)

	rand.Seed(time.Now().UnixNano())

	rserver := new(RServer)

	server := rpc.NewServer()
	server.Register(rserver)

	l, e := net.Listen("tcp", config.RpcIpPort)

	handleErrorFatal("listen error", e)
	outLog.Printf("Server started. Receiving on %s\n", config.RpcIpPort)
	fmt.Println("config:", config)

	// TODO
	go rescheduleFailedTask()
	go BackUpTask()

	serveRequest(l, server)

}

func loadTestData() (Xhat map[int][]float64) {
	Xhat = make(map[int][]float64)
	var Temp [][]float64
	dat, err := ioutil.ReadFile("./data/origindata.txt")
	if err != nil {
		fmt.Println("Read File error")
		os.Exit(1)
	}
	err = json.Unmarshal(dat, &Temp)
	if err != nil {
		fmt.Println("data parsing error")
		os.Exit(1)
	}
	n := len(Temp)
	for i := 0; i < n; i++ {
		Xhat[i] = Temp[i]
	}

	return Xhat
}

func serveRequest(l net.Listener, server *rpc.Server) {
	for {
		conn, _ := l.Accept()
		go server.ServeConn(conn)
	}
}

// Function to delete dead miners (no recent heartbeat)
func monitor(k string, heartBeatInterval time.Duration) {
	for {
		rnode, ok := allResourceNodes.all[k]
		if !ok {
			fmt.Println("MasterServer: Load node fail")
			return
		}

		if time.Now().UnixNano()-rnode.RecentHeartbeat > int64(heartBeatInterval) {
			outLog.Printf("%s timed out\n", rnode.Address.String())
			taskStatuses.taskStatusMutex.Lock()

			for work, _  := range taskStatuses.workerTaskMap[k] {
				if taskStatuses.allTasks[work] == false {
					taskStatuses.failedTaskQueue[work] = false
				}
			}
			delete(taskStatuses.workerTaskMap, k)
			delete(allResourceNodes.all, k)

			taskStatuses.taskStatusMutex.Unlock()
			return
		}
		//outLog.Printf("%s is alive\n", allResourceNodes.all[k].Address.String())
		time.Sleep(heartBeatInterval)
	}
}

// Registers a new miner with an address for other miner to use to
// connect to it (returned in GetNodes call below), and a
// public-key for this miner. Returns error, or if error is not set,
// then setting for this canvas instance.
//
// Returns:
// - AddressAlreadyRegisteredError if the server has already registered this address.
// - KeyAlreadyRegisteredError if the server already has a registration record for publicKey.

func (s *RServer) Register(request util.RegisterRequest, reply *util.RegisterReply) error {
	//v := ""
	//MasterLogger.UnpackReceive("Register request", request.Vbytes, &v)
	k := util.PubKeyToString(request.RInfo.Key)

	readPublicKey()
	_, ok := authorizedKeys[k]
	if !ok {
		fmt.Println("MasterServer: Not in authorized list", k)
		return errors.New("Not in authorized list")
	}
	if !ecdsa.Verify(&request.RInfo.Key, []byte(request.RInfo.Secret), request.RInfo.Sig.R, request.RInfo.Sig.S) {
		fmt.Println("MasterServer: Invalid Operation: Bad signature", request.RInfo)
		return errors.New("Invalid Operation: Bad signature")
	}
	c, err := rpc.Dial("tcp", request.RInfo.Address.String())
	if err != nil {
		fmt.Println("MasterServer: RPC connection error:", request.RInfo.Address.String())
		return errors.New("RPC connection error")
	}

	newRNode := &ResourceNode{
		request.RInfo.Address,
		time.Now().UnixNano(),
		c,
		0,
		k,
	}
	taskStatuses.taskStatusMutex.Lock()
	allResourceNodes.all[k]=newRNode
	taskStatuses.workerTaskMap[k] = make(map[int]bool)
	taskStatuses.taskStatusMutex.Unlock()// unlock?

	go monitor(k, time.Duration(config.ResourceNodeSettings.HeartBeat)*time.Millisecond)
	//bytes := MasterLogger.PrepareSend("Reply to register", "")
	*reply = util.RegisterReply{config.ResourceNodeSettings, nil}
	needToTrain = true
	outLog.Printf("Got Register from %s\n", request.RInfo.Address.String())

	return nil
}

func runMapTask(rnode *ResourceNode, taskNum int, kmeans map[int][]float64) {

	bytes := MasterLogger.PrepareSend("Master call Resource.DoTrainMapResourceNode", "")
	request := util.TrainMapRequest{globalModel.K, kmeans, bytes}
	reply := util.TrainMapReply{false, make([]byte, 0)}
	err := rnode.conn.Call("Resource.DoTrainMapResourceNode", request, &reply)
	v := ""
	MasterLogger.UnpackReceive("Return from calling Resource.DoTrainMapResourceNode", reply.Vbytes, &v)
	if err != nil {
		fmt.Println("MasterServer: Map task fail, resource node give up", rnode.Address.String())
		mapTaskChan <- TaskStatus{taskNum: taskNum, ok: false}
	} else {
		fmt.Println("MasterServer: Map task complete", rnode.Address.String())

		mapTasKMutex.Lock()

		mapTaskSuccessNodeAddress = append(mapTaskSuccessNodeAddress, rnode.Address)
		mapTasKMutex.Unlock()
		mapTaskChan <- TaskStatus{taskNum: taskNum, ok: true}
	}
}

type TaskStatus struct {
	taskNum    int
	ok         bool
	Mean       []float64
	KMeanError float64
	Stop       bool
	yi         int
	pubkey     string
}

func DoTrainMapServer(k int, kmeans map[int][]float64) {
	mapTaskSuccessNodeAddress = make([]net.Addr, 0)
	count := 0
	fmt.Println(allResourceNodes.all)
	for _, rnode := range allResourceNodes.all {
		fmt.Println(count)
		go runMapTask(rnode, count, kmeans)
		count++
	}

	taskMap := make(map[int]bool)
	completedTaskCount := 0
	for {
		taskStatus := <-mapTaskChan
		taskNum := taskStatus.taskNum
		if taskMap[taskNum] == false {
			completedTaskCount++
			taskMap[taskNum] = true
		}
		if completedTaskCount == count{
			break
		}
	}
}

func SplitWork(K int) (works map[string][]int) {
	count := 0
	works = make(map[string][]int)
	allAssigned := false
	for {
		for pubkey, _ := range allResourceNodes.all {
			if count == K {
				allAssigned = true
				break
			}
			works[pubkey] = append(works[pubkey], count)
			count++
		}
		if allAssigned {
			break
		}
	}
	fmt.Println("works", works)
	return works
}

func LoadBalancing(K int) (works map[string][]int) {
	// If this is the first time classifying, split work evenly
	if !splitWorkUsedinClassify {
		splitWorkUsedinClassify = true
		return SplitWork(K)
	}
	// Calculate average time from resourceNodeWorkTime
	var sumEfficiency float64
	taskAssigned := 0
	allAssigned := false
	var fastNodes []string
	var slowNodes []string
	works = make(map[string][]int)

	for _, v := range allResourceNodes.all {
		sumEfficiency += v.efficiency
	}
	avgEfficiency := sumEfficiency / float64(len(allResourceNodes.all))

	// Determine the fast and slowNodes
	for k1, v1 := range allResourceNodes.all {
		if v1.efficiency < avgEfficiency {
			fastNodes = append(fastNodes, k1)
		} else {
			slowNodes = append(slowNodes, k1)
		}
	}

	for {
		// Assign Work for fast nodes
		for _, pubkey := range fastNodes {
			if taskAssigned == K {
				allAssigned = true
				break
			}
			works[pubkey] = append(works[pubkey], taskAssigned)
			taskAssigned = taskAssigned + 1
			// For fast node, assign more task
			if taskAssigned < K {
				works[pubkey] = append(works[pubkey], taskAssigned)
				taskAssigned = taskAssigned + 1
			}
		}
		if allAssigned {
			return works
		}
		if K == taskAssigned {
			return works
		}

		// Assign Work for slow nodes
		if K > taskAssigned {
			for _, pubkey := range slowNodes {
				if taskAssigned == K {
					allAssigned = true
					break
				}
				works[pubkey] = append(works[pubkey], taskAssigned)
				taskAssigned++
			}
			if allAssigned {
				break
			}
		}
	}
	fmt.Println("Load Balancing", works)
	return works
}

func BackUpTask() {
	for {
		taskStatuses.taskStatusMutex.Lock()
		if !taskStatuses.startBackUpTask || taskStatuses.currentPhase == 0 {
			taskStatuses.taskStatusMutex.Unlock()
			continue
		}

		idleWorkers := make([]string, 0, 0)
		for pubkey, _ := range allResourceNodes.all {
			if len(taskStatuses.workerTaskMap[pubkey]) == 0 {
				idleWorkers = append(idleWorkers, pubkey)
			}
		}
		if len(idleWorkers) == 0 {
			//fmt.Println("Cannot find idle workers for backup tasks")
			taskStatuses.taskStatusMutex.Unlock()
			continue
		}

		backupTasks := make([]int, 0, 0)
		for taskNum, ok := range taskStatuses.allTasks {
			if !ok {
				backupTasks = append(backupTasks, taskNum)
			}
		}

		if len(backupTasks) == 0 {
			//fmt.Println("All tasks completed.")
			taskStatuses.taskStatusMutex.Unlock()
			continue
		}

		assigned := 0
		works := make(map[string][]int)
		allAssigned := false
		for {
			for _, pubkey := range idleWorkers {
				if assigned == len(backupTasks) {
					allAssigned = true
					break
				}
				taskStatuses.workerTaskMap[pubkey][backupTasks[assigned]] = true
				works[pubkey] = append(works[pubkey], backupTasks[assigned])
				assigned++
			}
			if allAssigned {
				break
			}
		}

		for pubkey, tasks := range works {
			if taskStatuses.currentPhase == 2 {
				go runReduceTask(tasks, allResourceNodes.all[pubkey])
			} else if taskStatuses.currentPhase == 3 {
				go runClassifyMapTask(allResourceNodes.all[pubkey], tasks, username, password, "xhat")
			}
		}
		taskStatuses.taskStatusMutex.Unlock()
		time.Sleep(3*time.Second)
	}
}

func runReduceTask(work []int, rnode *ResourceNode) {
	vbytes := MasterLogger.PrepareSend("Send Resource.DoTrainReduceResourceNode request", "")
	request := util.TrainReduceRequest{Work: work, AddressList: mapTaskSuccessNodeAddress, Vbyte: vbytes}
	reply := util.TrainReduceReply{Sces: make(map[int]util.SumCountError), Vbyte: make([]byte, 0)}
	fmt.Println("Contacting rnode for reduce task", rnode.Address.String())
	err := rnode.conn.Call("Resource.DoTrainReduceResourceNode", request, &reply)
	v := ""
	MasterLogger.UnpackReceive("Return from calling Resource.DoTrainReduceResourceNode", reply.Vbyte, &v)
	if err != nil {
		fmt.Println("MasterServer: Reduce task fail, resource node give up", rnode.Address.String())
		for _, w := range work {
			reduceTaskChan <- TaskStatus{taskNum: w, ok: false, pubkey:rnode.pubkey}
		}
	} else {
		fmt.Println("MasterServer: Reduce task complete", rnode.Address.String())
		fmt.Println("Worker return", reply)
		for _, w := range work {
			if reply.Stop {
				fmt.Println("MasterServer: Stop this iteration")
				reduceTaskChan <- TaskStatus{taskNum: w, ok: true, Stop: reply.Stop, pubkey:rnode.pubkey}
			} else {
				sce := reply.Sces[w]
				d := len(sce.Sum)
				for j := 0; j < d; j++ {
					sce.Sum[j] = sce.Sum[j] / float64(sce.Count)
				}
				reduceTaskChan <- TaskStatus{taskNum: w, ok: true, Mean: sce.Sum, KMeanError: sce.Err, Stop: reply.Stop, pubkey: rnode.pubkey}
			}
		}

	}
}

func resetTaskStatuses() {
	for pubkey, _ := range allResourceNodes.all {
		taskStatuses.workerTaskMap[pubkey] = make(map[int]bool)
	}
	taskStatuses.allTasks = make(map[int]bool)
	taskStatuses.failedTaskQueue = make(map[int]bool)
	taskStatuses.startBackUpTask = false
}

func DoTrainReduceServer(K int) (newMeans map[int][]float64, newKMeansError float64, stopIteration bool) {
	newMeans = make(map[int][]float64)
	newKMeansError = float64(0)
	initialWork := SplitWork(K)

	taskStatuses.taskStatusMutex.Lock()
	taskStatuses.currentPhase = 2

	for pubkey, works := range initialWork {
		for _, taskNum := range works {
			taskStatuses.workerTaskMap[pubkey][taskNum] = true
		}
	}
	taskStatuses.taskStatusMutex.Unlock()

	for pubkey, rnode := range allResourceNodes.all {
		go runReduceTask(initialWork[pubkey], rnode)
	}

	completedTaskCount := 0
	stopIteration = false
	backUpTaskStarted := false
	for {
		taskStatus := <-reduceTaskChan
		taskNum := taskStatus.taskNum

		taskStatuses.taskStatusMutex.Lock()

		delete(taskStatuses.workerTaskMap[taskStatus.pubkey], taskNum)
		if taskStatus.ok {
			if taskStatuses.allTasks[taskNum] == false {
				if taskStatus.Stop {
					stopIteration = true
				}
				completedTaskCount++
				newMeans[taskNum] = taskStatus.Mean
				newKMeansError = newKMeansError + taskStatus.KMeanError
				taskStatuses.allTasks[taskNum] = true
				delete(taskStatuses.failedTaskQueue, taskNum)
			}
			if completedTaskCount == K {
				taskStatuses.currentPhase = 0
				resetTaskStatuses()
				taskStatuses.taskStatusMutex.Unlock()
				break
			}
			//fmt.Println("testvalue")
			//fmt.Println(completedTaskCount)
			//fmt.Println(K)
			//fmt.Println(float32(completedTaskCount)/ float32(K))
			if !backUpTaskStarted && (float32(completedTaskCount)/ float32(K) > 0.8) {
				fmt.Println("Back up task started in reduce")
				taskStatuses.startBackUpTask = true
				backUpTaskStarted = true
			}
		} else {
			fmt.Println("MasterServer: Reduce task fail. ", taskNum)
			if !taskStatuses.allTasks[taskNum] {
				taskStatuses.failedTaskQueue[taskStatus.taskNum] = false
			}
		}
		taskStatuses.taskStatusMutex.Unlock()
	}
	return newMeans, newKMeansError, stopIteration
}

func rescheduleFailedTask() {
	for {
		time.Sleep(3*time.Second)
		//fmt.Println("RESCHEDULING !!!!")
		//fmt.Println(taskStatuses.failedTaskQueue)
		taskStatuses.taskStatusMutex.Lock()
		if taskStatuses.currentPhase == 0 {
			taskStatuses.taskStatusMutex.Unlock()
			continue
		}
		toAssign := make([]int, 0, 0)
		for taskNum, assigned := range taskStatuses.failedTaskQueue {
			if !assigned {
				fmt.Println("append to toassign",taskNum)
				toAssign = append(toAssign, taskNum)
			}
		}

		if len(toAssign) == 0 {
			taskStatuses.taskStatusMutex.Unlock()
			//fmt.Println("continue")
			continue
		}

		idleWorkers := make([]string, 0, 0)
		for pubkey, _ := range allResourceNodes.all {
			fmt.Println("forloop",pubkey)
			if len(taskStatuses.workerTaskMap[pubkey]) == 0 {
				fmt.Println("IDLE WORKER WOERK")
				idleWorkers = append(idleWorkers, pubkey)
			}
		}
		if len(idleWorkers) == 0 {
			//fmt.Println("Cannot find idle workers for failed tasks", toAssign)
			taskStatuses.taskStatusMutex.Unlock()
			continue
		}

		assigned := 0
		works := make(map[string][]int)
		allAssigned := false
		for {
			for _, pubkey := range idleWorkers {
				if assigned == len(toAssign) {
					allAssigned = true
					break
				}
				taskStatuses.failedTaskQueue[toAssign[assigned]] = true
				taskStatuses.workerTaskMap[pubkey][toAssign[assigned]] = true
				works[pubkey] = append(works[pubkey], toAssign[assigned])
				assigned++
				fmt.Println("lentoassign",len(toAssign))
				fmt.Println("see assigned",assigned)
				fmt.Println("see works",works)
			}
			if allAssigned {
				break
			}
		}
		fmt.Println("Reassigned failed tasks", works)
		for pubkey, tasks := range works {
			if taskStatuses.currentPhase == 2 {
				fmt.Println("test current 2")
				fmt.Println("pubkey",pubkey)
				fmt.Println("tasks",tasks)
				go runReduceTask(tasks, allResourceNodes.all[pubkey])
			} else if taskStatuses.currentPhase == 3 {
				fmt.Println("test current 3")
				go runClassifyMapTask(allResourceNodes.all[pubkey], tasks, username, password, "xhat")
			}
		}
		taskStatuses.taskStatusMutex.Unlock()
	}
}

func getPointsHelper(k int, r *ResourceNode, c chan[][]float64){
	//vbytes := MasterLogger.PrepareSend("Send Resource.GetPoints request", "")
	request := util.GetPointRequest{k, nil}
	reply := util.GetPointReply{make([][]float64, 0), make([]byte, 0)}
	err := r.conn.Call("Resource.GetPoints", request, &reply)
	if err != nil {
		fmt.Println("Get Point error", err)
	}
	//v := ""
	//MasterLogger.UnpackReceive("Return from calling Resource.GetPoints", reply.Vbyte, &v)
	c <- reply.Points
}



func getPointsfromResourceNode(K int) map[int][]float64 {
	points := make([][]float64, 0)
	chans := make([]chan [][]float64,0)
	for _, resource := range allResourceNodes.all {
		c := make(chan [][]float64)
		chans = append(chans, c)
		go getPointsHelper(K,resource,c)
	}
	for _,c := range chans{
		replyPoints := <- c
		points = append(points, replyPoints...)
	}
	numPoints := len(points)
	sampledIndex := make(map[int]bool)
	randomMean := make(map[int][]float64)
	for i := 0; i < K; i++ {
		newIndex := 0
		for {
			newIndex = rand.Intn(numPoints)
			_, ok := sampledIndex[newIndex]
			if !ok {
				randomMean[i] = points[newIndex]
				sampledIndex[newIndex] = true
				break
			}
		}
	}

	return randomMean
}

func initialTrainTask(K int) {
	globalModel.KMeansError = math.Inf(1)

	for i := 0; i < 30; i++ {

		skip := false
		//means := make(map[int][]float64)
		means := getPointsfromResourceNode(K)
		kmeansError := float64(0)


		fmt.Println("random mean", means)
		for ii := 0; ii < 15; ii++ {
			DoTrainMapServer(K, means)
			newMeans, newKMeansError, stopIteration := DoTrainReduceServer(K)
			if stopIteration {
				fmt.Println("stop iteration")
				skip = true
				break
			}
			fmt.Println("new means", newMeans)
			fmt.Println("new err", newKMeansError)
			means = newMeans
			kmeansError = newKMeansError
		}
		if skip {
			fmt.Println("continue")
			continue
		}
		if kmeansError < globalModel.KMeansError {
			globalModel.KMeansError = kmeansError
			globalModel.KMeans = means
			globalModel.K = K
		}

	}
	fmt.Println("golbalModel", globalModel.KMeans)
	fmt.Println("globalModel", globalModel.KMeansError)

}

type NewTaskRequest struct {
	K        int
	N        int
	Username string
	Password string
	Vbyte    []byte
}
type NewTaskReply struct {
	Err   bool
	Vbyte []byte
}

func (s *RServer) NewTask(request NewTaskRequest, reply *NewTaskReply) (err error) {
	v := ""
	MasterLogger.UnpackReceive("Receive NewTask request", request.Vbyte, &v)
	clientMutex.Lock()
	collection := "xhat"
	username = request.Username
	password = request.Password

	numAvailableNode := len(allResourceNodes.all)

	if numAvailableNode == 0 {

		reply.Vbyte = MasterLogger.PrepareSend("Reply to NewTask", "")
		return errors.New("No available resource node")
	}
	defer clientMutex.Unlock()
	if (request.K == globalModel.K) && !needToTrain {
		fmt.Println("MasterServer: start classifying.")
		//classify( request.Username,request.Password,collection,request.N)

	} else {
		fmt.Println("MasterServer: start training.")
		initialTrainTask(request.K)
		fmt.Println("MasterServer: start classifying.")
		classify(request.Username, request.Password, collection, request.N)
		needToTrain = false
	}
	reply.Vbyte = MasterLogger.PrepareSend("Reply to NewTask", "")

	return nil
}

func runClassifyMapTask(rnode *ResourceNode, assignedWorks []int, username string, password string, collection string) {
	//assingedTestData := make(map[int][]float64)
	vbytes := MasterLogger.PrepareSend("Send Resource.DoClassifyMapResourceNode request", "")
	request := util.ClassifyMapRequest{globalModel.KMeans, username, password, assignedWorks, collection, vbytes}
	reply := util.ClassifyMapReply{make(map[int]int), make([]byte, 0)}
	start := time.Now()

	err := rnode.conn.Call("Resource.DoClassifyMapResourceNode", request, &reply)
	elapsed := float64(time.Since(start).Nanoseconds())
	v := ""
	MasterLogger.UnpackReceive("Return from calling Resource.DoClassifyMapResourceNode", reply.Vbyte, &v)
	if err != nil {
		fmt.Println("MasterServer: Classify Map task fail, resource node give up", rnode.Address.String())
		if rnode.efficiency == 0 {
			rnode.efficiency = rnode.efficiency + elapsed
		} else {
			rnode.efficiency = (rnode.efficiency + elapsed) / 2
		}
		rnode.efficiency = (rnode.efficiency + elapsed) / 2
		for _, w := range assignedWorks {
			mapTaskChan <- TaskStatus{taskNum: w, ok: false, pubkey: rnode.pubkey}
		}
	} else {
		if rnode.efficiency == 0 {
			rnode.efficiency = elapsed / float64(len(assignedWorks)) / 2
		} else {
			rnode.efficiency = (rnode.efficiency + elapsed/float64(len(assignedWorks))) / 2
		}
		fmt.Println("MasterServer: Map task complete", rnode.Address.String(), "Elapsed time", elapsed)
		for _, w := range assignedWorks {
			mapTaskChan <- TaskStatus{taskNum: w, ok: true, yi: reply.Y[w], pubkey: rnode.pubkey}
		}
	}

}

func classify(username string, password string, collection string, n int) (y map[int]int) {
	//testData = Xhat
	//n := len(Xhat)
	works := LoadBalancing(n)

	taskStatuses.taskStatusMutex.Lock()
	taskStatuses.currentPhase = 3
	resetTaskStatuses()
	for pubkey, tasks := range works {
		for _, taskNum := range tasks {
			taskStatuses.workerTaskMap[pubkey][taskNum] = true
		}
	}
	taskStatuses.taskStatusMutex.Unlock()

	for pubkey, rnode := range allResourceNodes.all {
		go runClassifyMapTask(rnode, works[pubkey], username, password, collection)
	}

	y = make(map[int]int)
	completedTaskCount := 0
	backUpTaskStarted := false
	for {
		taskStatus := <-mapTaskChan
		taskNum := taskStatus.taskNum
		taskStatuses.taskStatusMutex.Lock()
		delete(taskStatuses.workerTaskMap[taskStatus.pubkey], taskNum)
		if taskStatus.ok{
			if taskStatuses.allTasks[taskNum] == false{
				completedTaskCount ++
				taskStatuses.allTasks[taskNum] = true
				delete(taskStatuses.failedTaskQueue, taskNum)
				y[taskNum] = taskStatus.yi
			}
			if completedTaskCount == n{
				taskStatuses.currentPhase = 0
				resetTaskStatuses()
				taskStatuses.taskStatusMutex.Unlock()
				break
			}

			if !backUpTaskStarted && (float32(completedTaskCount)/float32(n) >= 0.8) {
				fmt.Println("Back up task started in classify")
				taskStatuses.startBackUpTask = true
				backUpTaskStarted = true
			}
		}else{
			//////////////////////////////
			//reschedule work
			fmt.Println("MasterServer: Classify map task fail. Reschedule")
			//////////////////////////////
			if !taskStatuses.allTasks[taskNum] {
				taskStatuses.failedTaskQueue[taskStatus.taskNum] = false
			}
		}
		taskStatuses.taskStatusMutex.Unlock()
	}
	return y
}

type Addresses []net.Addr

func (a Addresses) Len() int           { return len(a) }
func (a Addresses) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Addresses) Less(i, j int) bool { return a[i].String() < a[j].String() }

// The server also listens for heartbeats from known miners. A miner must
// send a heartbeat to the server every HeartBeat milliseconds
// (specified in settings from server) after calling Register, otherwise
// the server will stop returning this miner's address/key to other
// miners.
//
// Returns:
// - UnknownKeyError if the server does not know a miner with this publicKey.
func (s *RServer) HeartBeat(key ecdsa.PublicKey, _ignored *bool) error {

	k := util.PubKeyToString(key)
	rnode, ok := allResourceNodes.all[k]

	if !ok {
		return unknownKeyError
	}

	rnode.RecentHeartbeat = time.Now().UnixNano()

	return nil
}
func (s *RServer) HelloWorld(str string, Reply *bool) error {
	fmt.Println("HelloWorld")
	*Reply = true
	return nil
}

func readPublicKey() {

	files, err := ioutil.ReadDir(config.ResourceNodeSettings.KeyDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {

		buf := make([]byte, 200)
		file, err := os.OpenFile(config.ResourceNodeSettings.KeyDir+f.Name(), os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			fmt.Println("MasterServer: read file error:", err)
		}
		file.Seek(0, 0)
		size, err := file.Read(buf)
		if err != nil {
			fmt.Println("MasterServer: read key error:", err)
		}
		buf = buf[:size]
		fmt.Println(size)
		pubkey, err := x509.ParsePKIXPublicKey(buf)
		if err != nil {
			fmt.Println("MasterServer: parse key error:", err)
		}
		key := pubkey.(*ecdsa.PublicKey)
		keystr := util.PubKeyToString(*key)
		authorizedKeys[keystr] = true
		file.Close()
	}
}
func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}

func handler1(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	rw.Header().Set("Access-Control-Allow-Headers",
		"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

	// Stop here if its Preflighted OPTIONS request
	if req.Method == "OPTIONS" {
		return
	}
	obj := FrontEndObj{X: resultX, Y: resulty}
	bytes, err := json.Marshal(obj)
	if err != nil {
		fmt.Println("err", err)

	}
	fmt.Println(string(bytes))
	fmt.Fprintf(rw, string(bytes))

}

type FrontEndObj struct {
	X map[int][]float64
	Y map[int]int
}
