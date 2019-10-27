//
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"os"
)

func main() {
	for i := 0; i < 20; i++ {
		priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		fmt.Println(err)
		a := priv.PublicKey
		pubBytes, err := x509.MarshalPKIXPublicKey(&a)
		fmt.Println("err", err)

		//fmt.Println(pubKeyToString(priv.PublicKey))
		istr := fmt.Sprintf("%v", i)
		file1, err := os.OpenFile("./pubkeys/pub"+istr, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			fmt.Println(err)
		}
		defer file1.Close()
		file1.Seek(0, 0)
		file1.Write(pubBytes)
		fmt.Println("key byte len:", len(pubBytes))
		privBytes, err := x509.MarshalECPrivateKey(priv)
		file2, err := os.OpenFile("./privkeys/priv"+istr, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			fmt.Println(err)
		}
		defer file2.Close()
		file2.Seek(0, 0)
		file2.Write(privBytes)
	}

}
