package util

import (
	"crypto/ecdsa"
	"math/big"
	"net"
)

type ResourceNodeInfo struct {
	Address net.Addr
	Key     ecdsa.PublicKey
	Sig     EcdsaSignature
	Secret  string
}
type EcdsaSignature struct {
	R, S *big.Int
}
type SumCountError struct {
	Sum   []float64
	Count int
	Err   float64
}
type MeanAndError struct {
	Mean []float64
	Err  float64
}

type TrainMapRequest struct {
	K      int
	KMeans map[int][]float64
	Vbytes []byte
}

type TrainMapReply struct {
	Err    bool
	Vbytes []byte
}

type TrainReduceRequest struct {
	Work        []int
	AddressList []net.Addr
	Vbyte       []byte
}
type ReturnSCERequest struct {
	Work  []int
	Vbyte []byte
}
type ReturnSCEReply struct {
	SCEMap map[int]SumCountError
	Vbyte  []byte
}

type TrainReduceReply struct {
	Sces  map[int]SumCountError
	Stop  bool
	Vbyte []byte
}
type GetPointReply struct {
	Points [][]float64
	Vbyte  []byte
}

type ClassifyMapRequest struct {
	KMeans map[int][]float64
	//Xhat   map[int][]float64
	Username   string
	Password   string
	Works      []int
	Collection string
	Vbyte      []byte
}
type ClassifyMapReply struct {
	Y     map[int]int
	Vbyte []byte
}

type GetPointRequest struct {
	K     int
	Vbyte []byte
}
type RegisterRequest struct {
	RInfo  ResourceNodeInfo
	Vbytes []byte
}
type RegisterReply struct {
	Rsetting ResourceNodeSettings
	Vbytes   []byte
}

type ResourceNodeSettings struct {
	// Number of milliseconds between heartbeat messages to the server.
	HeartBeat uint32 `json:"heartbeat"`
	KeyDir    string `json:"keydir"`
}
