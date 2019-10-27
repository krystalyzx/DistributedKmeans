package main

import (
	"./util"
	"crypto/ecdsa"
	"crypto/x509"
	"fmt"
	"os"
)

func main() {
	buf := make([]byte, 200)

	file, err := os.OpenFile("pubKey1", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	file.Seek(0, 0)
	size, err := file.Read(buf)
	fmt.Println(err)
	buf = buf[:size]
	privKey, err := x509.ParsePKIXPublicKey(buf)
	key := privKey.(*ecdsa.PublicKey)
	fmt.Println(err)
	fmt.Println(util.PubKeyToString(*key))

}
