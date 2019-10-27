package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"math/rand"
)

func PubKeyToString(key ecdsa.PublicKey) string {
	return string(elliptic.Marshal(key.Curve, key.X, key.Y))
}

func ConstructSVG(X map[int][]float64, y map[int]int, k int) string {
	start := "<svg width=\"1024\" height=\"1024\"><path d=\"M 1024 512 h -1024 M 512 1024 v -1024\" stroke=\"black\"/>"
	end := "</svg>"
	temp := start

	n := len(X)
	rgbarray := make([]string, 0)

	for i := 0; i < k; i++ {
		r := fmt.Sprintf("%v", rand.Intn(256))
		g := fmt.Sprintf("%v", rand.Intn(256))
		b := fmt.Sprintf("%v", rand.Intn(256))
		rgb := "rgb(" + r + "," + g + "," + b + ")"
		rgbarray = append(rgbarray, rgb)
	}

	for i := 0; i < n; i++ {
		a := 1024/2 + X[i][0]*20
		b := 1024/2 - X[i][1]*20
		temp = temp + ConstructSVGCircle(a, b, rgbarray[y[i]])
	}
	final := temp + end
	fmt.Println(final)
	return final
}

func ConstructSVGCircle(cx float64, cy float64, fill string) string {
	a := "<circle cx=\""
	b := "\" cy=\""
	c := "\" r=\"4\" fill=\""
	e := "\" />"
	cxx := fmt.Sprintf("%v", cx)
	cyy := fmt.Sprintf("%v", cy)
	return a + cxx + b + cyy + c + fill + e

}
