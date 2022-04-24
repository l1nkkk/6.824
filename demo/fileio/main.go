package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type KVPair struct {
	Key string `json:"Key"`
	Value string `json:"Value"`
}

func main(){
	var encRes []byte
	var err error
	var decRes []KVPair
	var file *os.File
	var fileRead *os.File
	var readData []byte
	kvs := []KVPair{
		{"ID1","111"},
		{"ID2","222"},
		{"ID3", "333"},
	}
	if encRes,err = json.Marshal(kvs); err != nil{
		panic("error")
	}
	if file,err = os.OpenFile("/tmp/jsonout", os.O_CREATE|os.O_WRONLY, 0644); err!=nil{
		panic("error")
	}
	file.Write(encRes)
	file.Close()

	if fileRead,err = os.Open("/tmp/jsonout"); err != nil{
		panic(err)
	}

	if readData,err = ioutil.ReadAll(fileRead); err != nil{
		panic(err)
	}
	fmt.Println(readData)
	if err = json.Unmarshal(readData, &decRes); err != nil{
		panic("error")
	}

	fmt.Println(decRes)
}
