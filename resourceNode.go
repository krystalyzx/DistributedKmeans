package main

import (
	"./util"
	"crypto/ecdsa"
	"crypto/elliptic"
	cyptoRand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var (
	privKey   *ecdsa.PrivateKey
	X         [][]float64
	svgStr    string
	id        string
	dialedMap map[string]*rpc.Client = make(map[string]*rpc.Client)
	RLogger   *govec.GoLog
)

type SquaredDist struct {
	MeanNum int
	SDist   float64
}

type Mean struct {
	MeanNum    int
	MeanVector []float64
}

type Data struct {
	Id       bson.ObjectId `bson:"_id,omitempty"`
	Xi       []float64
	Index    int
	Category int
}

type DimensionMissmatchError int

func (e DimensionMissmatchError) Error() string {
	return fmt.Sprintf("ResourceNode: Dimension missmatch (%d)", e)
}

func distToMeans(Xi []float64, Means map[int][]float64, d int) (dist2 map[int]float64, err error) {
	dist2 = make(map[int]float64)
	if len(Xi) != d {
		return nil, DimensionMissmatchError(len(Xi))
	}
	for meanNum, mean := range Means {
		sdist := float64(0)
		for j, xij := range Xi {
			jsdist := math.Pow(xij-mean[j], 2)
			sdist = sdist + jsdist
		}
		if math.IsNaN(sdist) {
			sdist = math.Inf(1)
		}
		dist2[meanNum] = sdist
	}
	return dist2, nil
}

func closestMean(dist2 map[int]float64) (ClosestMeanNum int) {
	minSDist := math.Inf(1)
	ClosestMeanNum = 0
	for meanNum, sdist := range dist2 {
		if sdist < minSDist {
			minSDist = sdist
			ClosestMeanNum = meanNum
		}
	}
	return ClosestMeanNum
}

func localKSumAndCount(X [][]float64, Means map[int][]float64, d int) (localKSumCountErrorMap map[int]util.SumCountError) {

	localKSumCountErrorMap = make(map[int]util.SumCountError, 0)
	for _, Xi := range X {
		//////////////////////////////////////////////////////////////////////////
		//test error later
		dist2, _ := distToMeans(Xi, Means, d)
		//////////////////////////////////////////////////////////////////////////
		closestMeanNum := closestMean(dist2)
		oldSumAndCountErr := localKSumCountErrorMap[closestMeanNum]

		if oldSumAndCountErr.Sum == nil {
			oldSumAndCountErr.Sum = make([]float64, d)
			oldSumAndCountErr.Count = 0
		}
		newSum := make([]float64, d)
		newError := oldSumAndCountErr.Err
		for j := 0; j < d; j++ {
			newSum[j] = oldSumAndCountErr.Sum[j] + Xi[j]
		}
		m := Means[closestMeanNum]
		for j := 0; j < d; j++ {
			a := Xi[j]
			b := m[j]
			c := a - b
			d := math.Pow(c, 2)
			//fmt.Println("before")
			//fmt.Println(kerr)
			newError = newError + d
			//fmt.Println("after")
			//fmt.Println(kerr)
		}

		newCount := oldSumAndCountErr.Count + 1
		localKSumCountErrorMap[closestMeanNum] = util.SumCountError{Sum: newSum, Count: newCount, Err: newError}
	}
	return localKSumCountErrorMap
}

func KMeansError(X [][]float64, Means map[int][]float64, d int) (kerr float64) {
	kerr = float64(0)
	for _, Xi := range X {

		//////////////////////////////////////////////////////////////////////////
		//test error later
		dist2, _ := distToMeans(Xi, Means, d)
		//////////////////////////////////////////////////////////////////////////
		closestMeanNum := closestMean(dist2)
		m := Means[closestMeanNum]
		//fmt.Println(m)
		for j := 0; j < d; j++ {
			a := Xi[j]
			b := m[j]
			c := a - b
			d := math.Pow(c, 2)
			//fmt.Println("before")
			//fmt.Println(kerr)
			kerr = kerr + d
			//fmt.Println("after")
			//fmt.Println(kerr)
		}

	}

	return kerr
}

func updateGlobalMean(scArray []util.SumCountError, d int) (newMean []float64) {
	globalSum := make([]float64, d)
	globalCount := 0
	for _, sc := range scArray {
		globalCount = globalCount + sc.Count
		for j := 0; j < d; j++ {
			globalSum[j] = sc.Sum[j]
		}
	}
	globalMean := make([]float64, d)
	for j := 0; j < d; j++ {
		globalMean[j] = globalSum[j] / float64(globalCount)
	}
	return globalMean
}
func KMeansPredict(X [][]float64, Means map[int][]float64, d int) (y []int) {
	n := len(X)
	y = make([]int, n)
	for i, Xi := range X {

		//////////////////////////////////////////////////////////////////////////
		//test error later
		dist2, _ := distToMeans(Xi, Means, d)
		//////////////////////////////////////////////////////////////////////////
		closestMeanNum := closestMean(dist2)
		y[i] = closestMeanNum
	}
	return y
}

func readData(dataPath string) {
	dat, _ := ioutil.ReadFile(dataPath)
	err := json.Unmarshal(dat, &X)
	if err != nil {
		fmt.Println("data parsing error")
		os.Exit(0)
	}
}

/*
func train(){

	n := len(X)
	d := 2
	k := 4
	var minMeans map[int][]float64
	var minError float64
	minError = math.Inf(1)
	for i := 0;i<50;i++{
		means := make(map[int][]float64)
		for kk := 0; kk < k ; kk++{
			ii := rand.Intn(n)
			means[kk] = X[ii]


		}
		for ii:= 0;ii<100;ii++{
			clusterXMap := localKSumAndCount(X,means,d)
			for meanNum, sc:= range clusterXMap{
				scArray := make([]util.SumCountError,0)
				scArray = append(scArray,sc)
				newMean := updateGlobalMean(scArray,d)
				means[meanNum] = newMean
			}
		}
		err := KMeansError(X,means,d)
		if err < minError{
			minError = err
			minMeans = means

		}
	}

	fmt.Println(minMeans)
	fmt.Println(KMeansError(X,minMeans,d))
	y := KMeansPredict(X,minMeans,d)
	svgStr = constructSVG(X,y,k)
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
*/

type Resource struct {
	privKey         *ecdsa.PrivateKey
	publicKey       ecdsa.PublicKey
	laddr           string
	setting         *util.ResourceNodeSettings
	sconn           *rpc.Client
	othersMsgSeqNum map[string]uint64
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func connectToDB(username string, password string) (*mgo.Session, error) {
	dialInfo := &mgo.DialInfo{
		Addrs:    []string{fmt.Sprintf("%s.documents.azure.com:10255", username)}, // Get HOST + PORT
		Timeout:  60 * time.Second,
		Database: username, // It can be anything
		Username: username, // Username
		Password: password, // PASSWORD
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), &tls.Config{})
		},
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	session, err := mgo.DialWithInfo(dialInfo)
	return session, err
}

func (r *Resource) DoClassifyMapResourceNode(request util.ClassifyMapRequest, reply *util.ClassifyMapReply) error {
	v := ""
	RLogger.UnpackReceive("Recieve DoClassifyMapResourceNode request", request.Vbyte, &v)
	reply.Y = make(map[int]int)

	//session,err0:= connectToDB("zackdb","7jC7us06u5TOgqc8J0QB3KepHlvnTns57nxoXeWvosVw9ckJlaAZAHSc0rEkGm6oK7xGAaVHFYhTMyP3fnESKQ==")

	session, err0 := connectToDB(request.Username, request.Password)

	if err0 != nil {
		fmt.Printf("Can't connect to mongo, go error %v\n", err0)
		os.Exit(1)
	}
	new_collection := session.DB(request.Username).C(request.Collection)


	// get a list from ClassifyMapRequest

	arr := request.Works
	//arr:= []int{0,3,4}

	// return data from db
	max := len(arr)
	var result []Data
	for i := 0; i < max; i = i + 200 {
		end := i + 200
		if end > max {
			end = max
		}
		arr := arr[i:end]
		err1 := new_collection.Find(bson.M{"index": bson.M{"$in": arr}}).All(&result)
		if err1 != nil {
			log.Fatal("Error finding record: ", err1)
		}

		//fmt.Println(result)
		for _, v := range result {
			//xi:= v.Xi
			xi := v.Xi

			index := v.Index
			dist2, err := distToMeans(xi, request.KMeans, 2)

			if err!=nil{
				fmt.Println("distToMeans err :", err)
			}
			/////////////////////////////////////////////////////////////f/////////////
			closestMeanNum := closestMean(dist2)
			err = new_collection.Update(bson.M{"index": index}, bson.M{"$set": bson.M{"cluster": closestMeanNum}})
			fmt.Println(index)
			if err != nil {
				if strings.Contains(err.Error(),"Request rate is large"){
					for {
						fmt.Println("sleep, request rate too large")
						time.Sleep(time.Millisecond)
						err = new_collection.Update(bson.M{"index": index}, bson.M{"$set": bson.M{"cluster": closestMeanNum}})
						if err == nil{
							break
						}
					}
				}else {

					fmt.Println("rqeust", request)
					fmt.Println("collection name", new_collection.Name)
					fmt.Println("Update Error: closetMeanNum", closestMeanNum, "dist2:", dist2)
					fmt.Print(" Update Error string  : ", err.Error())
					fmt.Println(strings.Contains(err.Error(),"Request rate is large"))
					log.Fatal(" Update Error : ", err)
					vbyte := RLogger.PrepareSend("Reply to DoClassifyMapResourceNode", "")
					reply.Vbyte = vbyte
					return nil
				}
			}

			reply.Y[index] = closestMeanNum
		}

	}
	fmt.Println("done")
	vbyte := RLogger.PrepareSend("Reply to DoClassifyMapResourceNode", "")
	reply.Vbyte = vbyte
	return nil
}

func heartbeat(m *Resource) {
	//heartbeat := time.Duration(m.setting.HeartBeat/2) * time.Millisecond
	heartbeat := time.Duration(500/2) * time.Millisecond
	_ignored := false
	for {
		select {
		case <-time.After(heartbeat):
			m.sconn.Call("RServer.HeartBeat", privKey.PublicKey, &_ignored)
		}
	}
}

func (r *Resource) GetPoints(request util.GetPointRequest, reply *util.GetPointReply) error {
	//v := ""
	//RLogger.UnpackReceive("Receive GetPoints request", request.Vbyte, &v)
	numPoints := len(X)
	reply.Points = make([][]float64, 0)
	sampledIndex := make(map[int]bool)

	for i := 0; i < request.K; i++ {
		newIndex := 0
		for {
			newIndex = rand.Intn(numPoints)
			_, ok := sampledIndex[newIndex]
			if !ok {
				reply.Points = append(reply.Points, X[newIndex])
				sampledIndex[newIndex] = true
				break
			}
		}
	}

	//vbytes := RLogger.PrepareSend("Reply to GetPoints", "")
	//reply.Vbyte = vbytes
	return nil
}

func (r *Resource) DoTrainReduceResourceNode(request util.TrainReduceRequest, reply *util.TrainReduceReply) error {
	v := ""
	RLogger.UnpackReceive("Receive DoTrainReduceResourceNode request from server", request.Vbyte, &v)
	sceAcc := make(map[int][]util.SumCountError)
	reply.Sces = make(map[int]util.SumCountError)
	for _, a := range request.AddressList {
		conn, ok := dialedMap[a.String()]
		if !ok {
			c, err := rpc.Dial("tcp", a.String())

			if err != nil {
				fmt.Println("ResourceNode : RPC connection error:", a.String(), err)
				//return errors.New("RPC connection error")
				continue
			}
			dialedMap[a.String()] = c
			conn = c
		}

		v1bytes := RLogger.PrepareSend("Send Resource.ReturnSumAndCountError request", "")
		request1 := util.ReturnSCERequest{request.Work, v1bytes}
		reply1 := util.ReturnSCEReply{make(map[int]util.SumCountError), make([]byte, 0)}
		err := conn.Call("Resource.ReturnSumAndCountError", request1, &reply1)
		v := ""
		RLogger.UnpackReceive("Return from calling Resource.ReturnSumAndCountError", reply1.Vbyte, &v)
		if err != nil {
			fmt.Println("ResourceNode: Fail to contact", a.String())
			delete(dialedMap, a.String())
			// redial
			fmt.Println("ResourceNode: Redial", a.String())
			x, err := rpc.Dial("tcp", a.String())
			if err != nil {
				fmt.Println("ResourceNode: Fail to redial", a.String())
			} else {
				dialedMap[a.String()] = x
				v1bytes := RLogger.PrepareSend("ReSend Resource.ReturnSumAndCountError request", "")
				request1 := util.ReturnSCERequest{request.Work, v1bytes}
				reply1 := util.ReturnSCEReply{make(map[int]util.SumCountError), make([]byte, 0)}
				err := conn.Call("Resource.ReturnSumAndCountError", request1, &reply1)
				if err != nil {
					fmt.Println("ResourceNode: Fail to contact", a.String())
					delete(dialedMap, a.String())
				}
				v := ""
				RLogger.UnpackReceive("Return from calling Resource.ReturnSumAndCountError", reply1.Vbyte, &v)
			}
		}
		for k, v := range reply1.SCEMap {
			if v.Count == 0 {
				continue
			}
			sceAcc[k] = append(sceAcc[k], v)
		}
	}
	//fmt.Println("DoTrainReduceResourceNode",sceAcc)
	for _, w := range request.Work {
		if sceAcc[w] == nil {
			fmt.Println("k", w, "has no Xi belongs to it, stop this iteration")
			reply.Stop = true
			vbytes := RLogger.PrepareSend("Reply to DoTrainReduceResourceNode", "")
			reply.Vbyte = vbytes
			return nil
		} else {
			v := sceAcc[w]
			globalSum := make([]float64, 2)
			//fmt.Println("globalsum",globalSum)
			fmt.Println(v)
			globalCount := 0
			globalErr := float64(0)
			for _, sc := range v {
				globalCount = globalCount + sc.Count
				globalErr = globalErr + sc.Err
				for j := 0; j < 2; j++ {
					globalSum[j] = globalSum[j] + sc.Sum[j]
				}
			}
			obj := util.SumCountError{Sum: globalSum, Count: globalCount, Err: globalErr}
			reply.Sces[w] = obj

		}
	}
	vbytes := RLogger.PrepareSend("Reply to DoTrainReduceResourceNode", "")
	reply.Vbyte = vbytes
	return nil
}

func (r *Resource) ReturnSumAndCountError(request util.ReturnSCERequest, reply *util.ReturnSCEReply) error {
	v := ""
	RLogger.UnpackReceive("Receive ReturnSumAndCountError request", request.Vbyte, &v)
	file, err := ioutil.ReadFile("./resourcenodefile/" + id + ".txt")
	if err != nil {
		fmt.Println(err)
	}
	var obj map[int]util.SumCountError
	err2 := json.Unmarshal(file, &obj)
	if err2 != nil {
		fmt.Println("ResourceNode: returnSumAndCountError marshal fail", obj)
	}

	returnMap := make(map[int]util.SumCountError)
	for _, v := range request.Work {
		returnMap[v] = obj[v]
	}
	vbtyes := RLogger.PrepareSend("Reply to ReturnSumAndCountError", "")
	reply.SCEMap = returnMap
	reply.Vbyte = vbtyes

	return nil
}

func (r *Resource) DoTrainMapResourceNode(request util.TrainMapRequest, reply *util.TrainMapReply) error {
	v := ""
	RLogger.UnpackReceive("Receive DoTrainMapResourceNode from server", request.Vbytes, &v)

	locakKSumCountErrorMap := localKSumAndCount(X, request.KMeans, 2)
	file, err := os.OpenFile("./resourcenodefile/"+id+".txt", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
	file.Truncate(0)
	file.Seek(0, 0)
	bytes, err := json.Marshal(locakKSumCountErrorMap)
	if err != nil {
		fmt.Println("ResourceNode: DoTrainMapResourceNode marshal fail", locakKSumCountErrorMap)
	}
	file.Write(bytes)
	vbytes := RLogger.PrepareSend("Reply to DoTrainMapResourceNode request", "")
	*reply = util.TrainMapReply{true, vbytes}
	file.Close()
	return nil
}

func main() {
	args := os.Args[1:]
	serverAddr := args[0]
	id = args[1]
	gob.Register(&net.TCPAddr{})
	gob.Register(&elliptic.CurveParams{})

	RLogger = govec.InitGoVector("resourceNode"+id, "resourceNode"+id+"LogFile")
	//RLogger.DisableLogging()
	//////////////////////////////////////////////////////////
	//change to argument lat
	readPrivKey("./privkeys/priv" + id)
	//////////////////////////////////////////////////////////
	//readData("./data/origindata" + id + ".txt")
	readData("./data/origin" + id + ".txt")
	//readData("./data/origindata.txt")
	conn, err := net.Dial("udp", serverAddr)
	localAddr := conn.LocalAddr().String()
	conn.Close()
	idx := strings.LastIndex(localAddr, ":")
	lIP := localAddr[0:idx]
	l, err := net.Listen("tcp", lIP+":0")
	handleError(err)

	resource := new(Resource)
	resource.laddr = fmt.Sprintf("%v:%v", lIP, l.Addr().(*net.TCPAddr).Port)

	fmt.Println("my address", resource.laddr)
	rpc.Register(resource)
	go rpc.Accept(l)
	laddr, err := net.ResolveTCPAddr("tcp", resource.laddr)
	handleError(err)
	fmt.Println(serverAddr)

	c, err := rpc.Dial("tcp", serverAddr)
	handleError(err)
	secret := "register"
	r, s, err := ecdsa.Sign(cyptoRand.Reader, privKey, []byte(secret))
	sig := util.EcdsaSignature{R: r, S: s}
	//vbytes := RLogger.PrepareSend("Register", "")
	RRequest := util.RegisterRequest{util.ResourceNodeInfo{Address: laddr, Key: privKey.PublicKey, Sig: sig, Secret: secret}, nil}
	RReply := util.RegisterReply{util.ResourceNodeSettings{}, make([]byte, 0)}
	err = c.Call("RServer.Register", RRequest, &RReply)
	handleError(err)
	//v := ""
	//RLogger.UnpackReceive("Reply from server", RReply.Vbytes, &v)

	resource.setting = &RReply.Rsetting
	resource.sconn = c
	//reply := false
	go heartbeat(resource)
	//resource.sconn.Call("RServer.HelloWorld", "", &reply)
	//train()
	time.Sleep(1 * time.Hour)

}

func readPrivKey(keypath string) {
	buf := make([]byte, 200)
	file, err := os.OpenFile(keypath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Println("ResourceNode: read file error:", err)
	}
	file.Seek(0, 0)
	size, err := file.Read(buf)
	if err != nil {
		fmt.Println("ResourceNode: read key error:", err)
	}
	buf = buf[:size]
	fmt.Println(size)
	pk, err := x509.ParseECPrivateKey(buf)
	if err != nil {
		fmt.Println("ResourceNode: parse key error:", err)
	}
	privKey = pk
	file.Close()
}
