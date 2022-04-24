package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)
var (
	Info *log.Logger
	Warning *log.Logger
	Error * log.Logger
)

func init(){
	errFile,err:=os.OpenFile("errors.logger",os.O_CREATE|os.O_WRONLY|os.O_APPEND,0666)
	if err!=nil{
		log.Fatalln("打开日志文件失败：",err)
	}

	Info = log.New(os.Stdout,"Info: ",log.Ldate | log.Lshortfile | log.Lmicroseconds)
	Warning = log.New(os.Stdout,"Warning:",log.Ldate | log.Ltime | log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr,errFile),"Error:",log.Ldate | log.Ltime | log.Lshortfile)

}

func main() {
	//Info.Println("飞雪无情的博客:","http://www.flysnow.org")
	//Warning.Printf("飞雪无情的微信公众号：%s\n","flysnow_org")
	//Error.Println("欢迎关注留言")
	////fmt.Println(os.Args[0])
	var l sync.Mutex
	for true{
		l.Lock()
		{
			defer fmt.Println("aaa")
		}

		break
	}
	fmt.Println("www")

	//l.Unlock()
}