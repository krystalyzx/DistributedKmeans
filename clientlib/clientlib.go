package clientlib

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"errors"
	"github.com/arcaneiceman/GoVector/govec"
)

type Client interface {
	Classify(k int, showResult bool) (err error)
	ShowResultFromDB() (err error)
}
type ClientObj struct {
	session    *mgo.Session
	username   string
	password   string
	serverConn *rpc.Client
}

var (
	X       map[int][]float64 = make(map[int][]float64)
	Y       map[int]int       = make(map[int]int)
	CLogger                   = govec.InitGoVector("Client", "ClientLogFile")
)

func Initial(username string, password string, serverAddr string) (c Client, err error) {
	//CLogger.DisableLogging()
	seesion, err := connectToDB(username, password)
	if err != nil {
		fmt.Println("ClientLib: connectToDB, connection fail", err)
		return nil, err
	}
	serverc, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("ClientLib: Cannot connect to server", serverAddr)
		return nil, err
	}
	c = ClientObj{session: seesion, username: username, password: password, serverConn: serverc}
	http.HandleFunc("/", handler1)
	////////////////////////////////////////////////////////////////////////////////////////
	//client use port 8081
	go http.ListenAndServe(":8081", nil)
	////////////////////////////////////////////////////////////////////////////////////////
	return c, nil
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

func (c ClientObj) Classify(k int, showResult bool) (err error) {
	collection := c.session.DB(c.username).C("xhat")
	n, err := collection.Find(nil).Count()
	if err != nil {
		fmt.Println("Classify: Fail to get the count", n)
		return err
	}
	if n == 0 {
		fmt.Println("Classify: 0 entry to classify")
		return errors.New("Classify: 0 entry to classify")
	}
	vbyte := CLogger.PrepareSend("Send RServer.NewTask request", "")
	request := NewTaskRequest{k, n, c.username, c.password, vbyte}
	reply := NewTaskReply{false, make([]byte, 0)}

	err = c.serverConn.Call("RServer.NewTask", request, &reply)
	if err != nil {
		fmt.Println("ClientLib: Classify fail", err)
		return err
	}
	if reply.Err != false {
		fmt.Println("ClientLib: Classify fail", err)
		return err
	}
	v := ""
	CLogger.UnpackReceive("Return from calling RServer.NewTask", reply.Vbyte, &v)
	if showResult {
		err = c.ShowResultFromDB()
		if err != nil{
			fmt.Println("Classify show DB error")
		}
	}

	return nil
}

type Data struct {
	Id      bson.ObjectId `bson:"_id,omitempty"`
	Xi      []float64
	Index   int
	Cluster int
}

func (c ClientObj) Close() {
	c.session.Close()
}

func (c ClientObj) ShowResultFromDB() (err error) {
	XY_collection := c.session.DB(c.username).C("xhat")
	XY := make([]Data, 0)
	err = XY_collection.Find(nil).All(&XY)
	if err != nil {
		fmt.Println("ShowResultFromDB: Fail to find X", err)
		return err
	}
	X = make(map[int][]float64)
	Y = make(map[int]int)

	for _, xi := range XY {
		index := xi.Index
		X[index] = xi.Xi
		Y[index] = xi.Cluster
	}
	fmt.Println("ShowResultFromDB: Ready to display result")
	return nil
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
	obj := FrontEndObj{X: X, Y: Y}
	bytes, err := json.Marshal(obj)
	if err != nil {
		fmt.Println("handler err", err)
	}
	fmt.Println(string(bytes))
	fmt.Fprintf(rw, string(bytes))
}

type FrontEndObj struct {
	X map[int][]float64
	Y map[int]int
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
