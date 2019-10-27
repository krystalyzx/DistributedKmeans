package main

import (
	"fmt"
	//	"net/rpc"

	"./clientlib"
	"os"

	"time"
	"strconv"
)

func main() {
	args := os.Args[1:]
	serverAddr := args[0]
	k_str:= args[1]

	//readData("./data/origindata.txt")
	fmt.Println("server address: ", serverAddr)
	c, err := clientlib.Initial("zackdb", "7jC7us06u5TOgqc8J0QB3KepHlvnTns57nxoXeWvosVw9ckJlaAZAHSc0rEkGm6oK7xGAaVHFYhTMyP3fnESKQ==", serverAddr)
	if err != nil {
		fmt.Println("Initial error: ", err)
		os.Exit(1)
	}
	k,err:=strconv.Atoi(k_str)
	if err != nil {
		fmt.Println("str to int error: ", err)
	}

	err = c.Classify( k, true)
	if err != nil {
		fmt.Println("Classify error: ", err)
	}
	time.Sleep(time.Hour)

}
