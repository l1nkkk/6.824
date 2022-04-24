package logger

import (
	"log"
	"os"
)

var (
	Info *log.Logger
	Warning *log.Logger
	Error * log.Logger
	Panic * log.Logger
)

func init(){
	//errFile,err:=os.OpenFile("errors.logger",os.O_CREATE|os.O_WRONLY|os.O_APPEND,0666)
	//if err!=nil{
	//	logger.Fatalln("打开日志文件失败：",err)
	//}

	Info = log.New(os.Stdout,"Info: ",log.Ldate | log.Lmicroseconds | log.Lshortfile)
	Warning = log.New(os.Stdout,"Warning: ",log.Ldate | log.Lmicroseconds | log.Lshortfile)
	Error = log.New(os.Stderr,"Error: ",log.Ldate | log.Lmicroseconds | log.Lshortfile)
	Panic = log.New(os.Stderr, "Panic: ", log.Ldate | log.Lmicroseconds | log.Lshortfile)
	//Error = logger.New(io.MultiWriter(os.Stderr,errFile),"Error:",logger.Ldate | logger.Ltime | logger.Lshortfile)

}